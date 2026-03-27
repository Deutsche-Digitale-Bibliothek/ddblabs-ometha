import asyncio
import os
import subprocess
import sys
import tkinter as tk
from pathlib import Path
from tkinter import filedialog

import requests
import urllib3
from lxml import etree
from nicegui import app, run, ui

from .harvester import get_identifier, harvest_files
from .helpers import NAMESPACE, TIMESTR


def _build_session() -> requests.Session:
    from requests.adapters import HTTPAdapter

    adapter = HTTPAdapter(
        max_retries=urllib3.util.retry.Retry(
            total=8,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=3,
            raise_on_status=False,
        )
    )
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    return session


def _fetch_metadata_formats(b_url: str, session) -> list[str]:
    resp = session.get(f"{b_url}?verb=ListMetadataFormats", verify=False, timeout=(10, 30))
    root = etree.XML(resp.content)
    return [el.text for el in root.findall(f".//{NAMESPACE}metadataPrefix") if el.text]


def _fetch_sets(b_url: str, session) -> dict[str, str]:
    """Returns {setSpec: label} – handles ResumptionToken pagination."""
    sets: dict[str, str] = {}
    url: str | None = f"{b_url}?verb=ListSets"
    while url:
        resp = session.get(url, verify=False, timeout=(10, 30))
        root = etree.XML(resp.content)
        if root.find(f".//{NAMESPACE}error") is not None:
            break
        for set_el in root.findall(f".//{NAMESPACE}set"):
            spec = set_el.findtext(f"{NAMESPACE}setSpec") or ""
            name = set_el.findtext(f"{NAMESPACE}setName") or spec
            if spec:
                sets[spec] = f"{name}  [{spec}]" if name != spec else spec
        token = root.findtext(f".//{NAMESPACE}resumptionToken")
        url = f"{b_url}?verb=ListSets&resumptionToken={token}" if token else None
    return sets


@ui.page("/")
def index():
    with ui.column().classes("w-full max-w-2xl mx-auto p-6 gap-4"):

        # Header
        with ui.row().classes("items-baseline gap-3"):
            ui.label("Ometha").classes("text-3xl font-bold")
            ui.label("OAI-PMH Harvester").classes("text-gray-400")

        # Pflichtfelder
        with ui.card().classes("w-full"):
            ui.label("Pflichtfelder").classes("font-semibold mb-2")
            with ui.row().classes("w-full gap-2 items-end"):
                b_url = ui.input("Base-URL", placeholder="https://oai.deutsche-digitale-bibliothek.de").classes("flex-1")
                fetch_btn = ui.button(icon="travel_explore").props("flat dense").tooltip("Sets & Formate laden")
            fetch_status = ui.label("").classes("text-xs text-gray-400 -mt-2")
            pref = ui.select(
                options=[],
                label="Metadata Prefix",
                with_input=True,
                new_value_mode="add-unique",
            ).classes("w-full")

        # Optionale Felder
        with ui.card().classes("w-full"):
            ui.label("Optionale Felder").classes("font-semibold mb-2")
            dat_geb = ui.input("Datengeber (Ordnername)", placeholder=TIMESTR).classes("w-full")
            with ui.row().classes("w-full gap-2 items-end"):
                out_f = ui.input(
                    "Ausgabeordner",
                    value=str(Path.home() / "Downloads"),
                ).classes("flex-1")
                ui.button(icon="folder_open").props("flat dense").on(
                    "click", lambda: pick_folder()
                )
            sets_select = ui.select(
                options={},
                label="Set(s)",
                multiple=True,
                with_input=True,
            ).props("use-chips outlined").classes("w-full")
            with ui.row().classes("w-full gap-4"):
                with ui.input("Von-Datum (YYYY-MM-DD)", placeholder="YYYY-MM-DD").classes("flex-1") as f_date:
                    with f_date.add_slot("append"):
                        ui.icon("event").classes("cursor-pointer").on("click", lambda: f_date_menu.open())
                    with ui.menu() as f_date_menu:
                        ui.date(mask="YYYY-MM-DD").bind_value(f_date)
                with ui.input("Bis-Datum (YYYY-MM-DD)", placeholder="YYYY-MM-DD").classes("flex-1") as u_date:
                    with u_date.add_slot("append"):
                        ui.icon("event").classes("cursor-pointer").on("click", lambda: u_date_menu.open())
                    with ui.menu() as u_date_menu:
                        ui.date(mask="YYYY-MM-DD").bind_value(u_date)
            with ui.row().classes("w-full gap-4"):
                n_procs = ui.number("Parallele Downloads", value=4, min=1, max=100).classes("flex-1")
                timeout = ui.number("Timeout (s)", value=0, min=0).classes("flex-1")
            exp_type = ui.select(["xml", "json"], value="xml", label="Exportformat").classes("w-full")

        # Fehleranzeige
        error_banner = ui.label("").classes("text-red-600 font-medium hidden")

        # Aktionszeile
        with ui.row().classes("items-center gap-4"):
            start_btn = ui.button("Harvesting starten", icon="play_arrow").classes("bg-green-600 text-white")
            spinner = ui.spinner(size="lg").classes("hidden")
            status_label = ui.label("").classes("text-gray-500 text-sm flex-1")
            async def quit_app():
                with ui.dialog() as confirm_dialog, ui.card():
                    ui.label("Ometha wirklich beenden?").classes("font-semibold")
                    with ui.row().classes("gap-2 mt-2"):
                        ui.button("Ja, beenden", icon="power_settings_new").classes("bg-red-600 text-white").on(
                            "click", lambda: confirm_dialog.submit("yes")
                        )
                        ui.button("Abbrechen").props("flat").on(
                            "click", lambda: confirm_dialog.submit("no")
                        )
                result = await confirm_dialog
                if result == "yes":
                    await ui.run_javascript("window.close()")
                    app.shutdown()

            ui.button("Beenden", icon="power_settings_new").props("flat").classes("text-red-500").on(
                "click", quit_app
            )

        # Log-Ausgabe
        log_area = ui.log(max_lines=500).classes("w-full h-64 font-mono text-sm border rounded")

    # ---------------------------------------------------------------------------

    def show_error(msg: str):
        error_banner.set_text(msg)
        error_banner.classes(remove="hidden")

    def hide_error():
        error_banner.classes(add="hidden")

    def log(msg: str):
        log_area.push(msg)
        status_label.set_text(msg)

    async def fetch_endpoint_info():
        url = b_url.value.strip().rstrip("/")
        if not url:
            return
        fetch_status.set_text("Lade Endpoint-Informationen …")
        fetch_btn.disable()
        try:
            session = _build_session()
            prefixes, sets = await asyncio.gather(
                run.io_bound(_fetch_metadata_formats, url, session),
                run.io_bound(_fetch_sets, url, session),
            )
            pref.set_options(prefixes, value=prefixes[0] if prefixes else None)
            sets_select.set_options(sets)
            n_sets = len(sets)
            fetch_status.set_text(
                f"✓  {len(prefixes)} Formate"
                + (f",  {n_sets} Sets" if n_sets else ",  keine Sets")
            )
        except Exception as e:
            fetch_status.set_text(f"Fehler beim Laden: {e}")
        finally:
            fetch_btn.enable()

    async def pick_folder():
        def _dialog():
            if sys.platform == "darwin":
                proc = subprocess.run(
                    [
                        "osascript", "-e",
                        'POSIX path of (choose folder with prompt "Ausgabeordner wählen")',
                    ],
                    capture_output=True,
                    text=True,
                )
                return proc.stdout.strip() if proc.returncode == 0 else None
            else:
                root = tk.Tk()
                root.withdraw()
                root.wm_attributes("-topmost", True)
                path = filedialog.askdirectory(
                    title="Ausgabeordner wählen",
                    initialdir=out_f.value or str(Path.home() / "Downloads"),
                )
                root.destroy()
                return path or None

        result = await run.io_bound(_dialog)
        if result:
            out_f.set_value(result)

    async def start_harvesting():
        hide_error()
        log_area.clear()

        if not b_url.value.strip():
            show_error("Base-URL ist erforderlich.")
            return
        if not pref.value:
            show_error("Metadata Prefix ist erforderlich.")
            return

        selected_sets = list(sets_select.value or [])

        # PRM aufbauen
        prm = {
            "b_url":     b_url.value.strip().rstrip("/"),
            "pref":      pref.value if isinstance(pref.value, str) else str(pref.value),
            "dat_geb":   dat_geb.value.strip() or TIMESTR,
            "out_f":     out_f.value.strip() or str(Path.home() / "Downloads"),
            "sets":      [{"additive": selected_sets, "intersection": []}],
            "f_date":    f_date.value.strip() or None,
            "u_date":    u_date.value.strip() or None,
            "n_procs":   int(n_procs.value or 16),
            "timeout":   float(timeout.value or 0),
            "exp_type":  exp_type.value,
            "debug":     False,
            "res_tok":   None,
            "id_f":      None,
            "conf_f":    None,
            "conf_m":    False,
            "auto_m":    False,
            "mode":      "cli",
        }

        # Ausgabeordner anlegen
        folder = os.path.join(prm["out_f"], prm["dat_geb"], TIMESTR)
        folder = folder.replace(":", "_")
        os.makedirs(folder, exist_ok=True)

        # UI: laufend
        start_btn.disable()
        spinner.classes(remove="hidden")

        session = _build_session()

        try:
            # Phase 1: Identifier sammeln
            log("Sammle Identifier …")
            harvest_url = f"{prm['b_url']}?verb=ListIdentifiers&metadataPrefix={prm['pref']}"
            if prm["f_date"]:
                harvest_url += f"&from={prm['f_date']}"
            if prm["u_date"]:
                harvest_url += f"&until={prm['u_date']}"
            if prm["sets"] and prm["sets"][0].get("additive"):
                harvest_url += f"&set={prm['sets'][0]['additive'][0]}"

            def on_list_size(n: int):
                log(f"Angegebene ListSize: {n}")

            ids = await run.io_bound(get_identifier, prm, harvest_url, session, on_list_size)
            log(f"{len(ids)} Identifier gefunden.")

            # Phase 2: Dateien herunterladen
            log("Starte Datei-Harvesting …")
            failed_dl, failed_ids = await run.io_bound(harvest_files, ids, prm, folder, session)

            # Ergebnis
            n_ok = len(ids) - len(failed_dl) - len(failed_ids)
            log(f"Fertig: {n_ok}/{len(ids)} Dateien gespeichert → {folder}")
            if failed_dl or failed_ids:
                show_error(
                    f"{len(failed_dl) + len(failed_ids)} Datensätze konnten nicht geharvestet werden."
                )

        except SystemExit:
            show_error("Abbruch – Schnittstelle nicht erreichbar?")
        except Exception as e:
            show_error(f"Fehler: {e}")
        finally:
            start_btn.enable()
            spinner.classes(add="hidden")

    b_url.on("blur", fetch_endpoint_info)
    fetch_btn.on("click", fetch_endpoint_info)
    start_btn.on_click(start_harvesting)


def start_gui():
    ui.run(title="Ometha", port=8765, reload=False)
