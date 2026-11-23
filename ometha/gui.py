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

from .harvester import get_identifier, harvest_files, preview_list_size
from .helpers import NAMESPACE, TIMESTR, safe_get
from ._version import __version__ as omethaversion
from .i18n import t


N_PROCS_DEFAULT = 4  # Vorgabewert im Formular


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
    resp = safe_get(session, f"{b_url}?verb=ListMetadataFormats", timeout=(10, 30))
    root = etree.XML(resp.content)
    return [el.text for el in root.findall(f".//{NAMESPACE}metadataPrefix") if el.text]


def _fetch_sets(b_url: str, session) -> dict[str, str]:
    """Returns {setSpec: label} – handles ResumptionToken pagination."""
    sets: dict[str, str] = {}
    url: str | None = f"{b_url}?verb=ListSets"
    while url:
        resp = safe_get(session, url, timeout=(10, 30))
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
            ui.label(t("gui.oai_pmh_harvester")).classes("text-gray-400")
            ui.label(f"v{omethaversion}").classes("text-gray-400 text-sm")

        # Pflichtfelder
        with ui.card().classes("w-full"):
            ui.label(t("gui.required_fields")).classes("font-semibold mb-2")
            with ui.row().classes("w-full gap-2 items-end"):
                b_url = ui.input(
                    t("gui.base_url"),
                    placeholder="https://oai.deutsche-digitale-bibliothek.de",
                ).classes("flex-1")
                fetch_btn = ui.button(icon="travel_explore").props("flat dense").tooltip(t("gui.load_sets_tooltip"))
            fetch_status = ui.label("").classes("text-xs text-gray-400 -mt-2")
            pref = ui.select(
                options=[],
                label=t("gui.metadata_prefix"),
                with_input=True,
                new_value_mode="add-unique",
            ).classes("w-full")

        # Optionale Felder
        with ui.card().classes("w-full"):
            ui.label(t("gui.optional_fields")).classes("font-semibold mb-2")
            dat_geb = ui.input(t("gui.data_provider"), placeholder=TIMESTR).classes("w-full")
            with ui.row().classes("w-full gap-2 items-end"):
                out_f = ui.input(
                    t("gui.output_folder"),
                    value=str(Path.home() / "Downloads"),
                ).classes("flex-1")
                ui.button(icon="folder_open").props("flat dense").on("click", lambda: pick_folder())
            sets_select = (
                ui.select(
                    options={},
                    label=t("gui.sets"),
                    multiple=True,
                    with_input=True,
                )
                .props("use-chips outlined")
                .classes("w-full")
            )
            with ui.row().classes("w-full gap-4"):
                with ui.input(t("gui.from_date"), placeholder="YYYY-MM-DD").classes("flex-1") as f_date:
                    with f_date.add_slot("append"):
                        ui.icon("event").classes("cursor-pointer").on("click", lambda: f_date_menu.open())
                    with ui.menu() as f_date_menu:
                        ui.date(mask="YYYY-MM-DD").bind_value(f_date)
                with ui.input(t("gui.until_date"), placeholder="YYYY-MM-DD").classes("flex-1") as u_date:
                    with u_date.add_slot("append"):
                        ui.icon("event").classes("cursor-pointer").on("click", lambda: u_date_menu.open())
                    with ui.menu() as u_date_menu:
                        ui.date(mask="YYYY-MM-DD").bind_value(u_date)
            with ui.row().classes("w-full gap-4"):
                n_procs = ui.number(t("gui.parallel_downloads"), value=N_PROCS_DEFAULT, min=1, max=100).classes("flex-1")
                timeout = ui.number(t("gui.timeout_s"), value=0, min=0).classes("flex-1")
                batch_size = ui.number(t("gui.files_per_subfolder"), value=None, min=1).classes("flex-1")
            exp_type = ui.select(["xml", "json"], value="xml", label=t("gui.export_format")).classes("w-full")
            deleted_handling = ui.select(
                ["keep", "skip", "folder"],
                value="keep",
                label=t("gui.deleted_records"),
            ).classes("w-full")
            with ui.row().classes("w-full gap-4 items-center"):
                gentle = ui.switch(t("gui.gentle_mode")).classes("flex-1")
                ratelimit = ui.number(t("gui.rate_limit_s"), value=0, min=0).classes("flex-1").tooltip(
                    t("gui.rate_limit_tooltip")
                )

        # ListSize-Vorschau
        with ui.row().classes("w-full items-center gap-2"):
            listsize_btn = ui.button(t("gui.check_listsize"), icon="query_stats").props("outline")
            listsize_label = ui.label("").classes("text-gray-500 text-sm")

        # Fehleranzeige
        error_banner = ui.label("").classes("text-red-600 font-medium hidden")

        # Aktionszeile
        with ui.row().classes("items-center gap-4"):
            start_btn = ui.button(t("gui.start_harvesting"), icon="play_arrow").classes("bg-green-600 text-white")
            spinner = ui.spinner(size="lg").classes("hidden")
            status_label = ui.label("").classes("text-gray-500 text-sm flex-1")

            async def quit_app():
                with ui.dialog() as confirm_dialog, ui.card():
                    ui.label(t("gui.confirm_quit")).classes("font-semibold")
                    with ui.row().classes("gap-2 mt-2"):
                        ui.button(t("gui.quit_yes"), icon="power_settings_new").classes("bg-red-600 text-white").on(
                            "click", lambda: confirm_dialog.submit("yes")
                        )
                        ui.button(t("gui.cancel")).props("flat").on("click", lambda: confirm_dialog.submit("no"))
                result = await confirm_dialog
                if result == "yes":
                    await ui.run_javascript("window.close()")
                    app.shutdown()

            ui.button(t("gui.quit"), icon="power_settings_new").props("flat").classes("text-red-500").on("click", quit_app)

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
        fetch_status.set_text(t("gui.loading_endpoint"))
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
            fetch_status.set_text(t("gui.endpoint_summary", formats=len(prefixes), sets=n_sets) if n_sets else t("gui.endpoint_summary_no_sets", formats=len(prefixes)))
        except Exception as e:
            fetch_status.set_text(t("gui.load_error", error=e))
        finally:
            fetch_btn.enable()

    async def check_list_size():
        url = b_url.value.strip().rstrip("/")
        prefix = pref.value if isinstance(pref.value, str) else str(pref.value or "")
        if not url or not prefix:
            listsize_label.set_text(t("gui.fill_url_and_prefix"))
            return
        listsize_label.set_text(t("gui.checking_listsize"))
        listsize_btn.disable()
        try:
            session = _build_session()

            base_url = f"{url}?verb=ListIdentifiers&metadataPrefix={prefix}"
            if f_date.value.strip():
                base_url += f"&from={f_date.value.strip()}"
            if u_date.value.strip():
                base_url += f"&until={u_date.value.strip()}"

            selected_sets = list(sets_select.value or [])
            if selected_sets:
                sizes = await asyncio.gather(
                    *[
                        run.io_bound(preview_list_size, f"{base_url}&set={s}", session)
                        for s in selected_sets
                    ]
                )
                if any(size is None for size in sizes):
                    known_sum = sum(size for size in sizes if size is not None)
                    unknown_count = sum(1 for size in sizes if size is None)
                    known_sum_str = f"{known_sum:,}".replace(",", ".")
                    listsize_label.set_text(
                        t("gui.listsize_at_least", total=known_sum_str, unknown=unknown_count, sets=len(selected_sets))
                    )
                else:
                    listsize_label.set_text(t("gui.listsize_sum", total=f"{sum(sizes):,}".replace(",", "."), sets=len(selected_sets)))
            else:
                size = await run.io_bound(preview_list_size, base_url, session)
                listsize_label.set_text(t("gui.listsize_approx", total=f"{size:,}".replace(",", ".")) if size is not None else t("gui.listsize_not_given"))
        except Exception as e:
            listsize_label.set_text(t("gui.listsize_error", error=e))
        finally:
            listsize_btn.enable()

    async def pick_folder():
        def _dialog():
            if sys.platform == "darwin":
                prompt = t("gui.choose_folder_title")
                proc = subprocess.run(
                    [
                        "osascript",
                        "-e",
                        f'POSIX path of (choose folder with prompt "{prompt}")',
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
                    title=t("gui.choose_folder_title"),
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
            show_error(t("gui.base_url_required"))
            return
        if not pref.value:
            show_error(t("gui.prefix_required"))
            return

        selected_sets = list(sets_select.value or [])

        # PRM aufbauen
        prm = {
            "b_url": b_url.value.strip().rstrip("/"),
            "pref": pref.value if isinstance(pref.value, str) else str(pref.value),
            "dat_geb": dat_geb.value.strip() or TIMESTR,
            "out_f": out_f.value.strip() or str(Path.home() / "Downloads"),
            "sets": [{"additive": selected_sets, "intersection": []}],
            "f_date": f_date.value.strip() or None,
            "u_date": u_date.value.strip() or None,
            "n_procs": int(n_procs.value or 16),
            "timeout": float(timeout.value or 0),
            "batch_size": int(batch_size.value) if batch_size.value else None,
            "exp_type": exp_type.value,
            "deleted_handling": deleted_handling.value,
            "gentle": gentle.value,
            "ratelimit": float(ratelimit.value) if ratelimit.value else None,
            "debug": False,
            "res_tok": None,
            "id_f": None,
            "conf_f": None,
            "conf_m": False,
            "auto_m": False,
            "mode": "cli",
        }

        # Gentle Mode: Defaults setzen falls nicht explizit überschrieben
        from .helpers import GENTLE_DEFAULTS
        if prm["gentle"]:
            if prm["n_procs"] == N_PROCS_DEFAULT:  # nicht vom Nutzer geändert
                prm["n_procs"] = GENTLE_DEFAULTS["n_procs"]
            if not prm["timeout"]:
                prm["timeout"] = GENTLE_DEFAULTS["timeout"]
            if not prm["ratelimit"]:
                prm["ratelimit"] = GENTLE_DEFAULTS["ratelimit"]

        # Ausgabeordner anlegen
        safe_timestr = TIMESTR.replace(":", "_")
        folder = os.path.join(prm["out_f"], prm["dat_geb"], safe_timestr)
        os.makedirs(folder, exist_ok=True)

        # UI: laufend
        start_btn.disable()
        spinner.classes(remove="hidden")

        session = _build_session()

        try:
            # Phase 1: Identifier sammeln
            base_harvest_url = f"{prm['b_url']}?verb=ListIdentifiers&metadataPrefix={prm['pref']}"
            if prm["f_date"]:
                base_harvest_url += f"&from={prm['f_date']}"
            if prm["u_date"]:
                base_harvest_url += f"&until={prm['u_date']}"

            additive_sets = prm["sets"][0].get("additive") if prm["sets"] else []

            def on_list_size(n: int):
                log(t("gui.listsize_given", count=n))

            if additive_sets:
                ids = []
                for set_spec in additive_sets:
                    log(t("gui.collect_ids_set", set_name=set_spec))
                    set_url = base_harvest_url + f"&set={set_spec}"
                    set_ids = await run.io_bound(get_identifier, prm, set_url, session, on_list_size)
                    log(t("gui.ids_in_set", count=len(set_ids), set_name=set_spec))
                    ids.extend(set_ids)
                ids = list(dict.fromkeys(ids))
            else:
                log(t("gui.collect_ids"))
                ids = await run.io_bound(get_identifier, prm, base_harvest_url, session, on_list_size)
            log(t("gui.ids_found", count=len(ids)))

            # Phase 2: Dateien herunterladen
            log(t("gui.starting_file_harvest"))
            failed_dl, failed_ids = await run.io_bound(harvest_files, ids, prm, folder, session)

            # Ergebnis
            n_ok = len(ids) - len(failed_dl) - len(failed_ids)
            log(t("gui.done", ok=n_ok, total=len(ids), folder=folder))
            if failed_dl or failed_ids:
                show_error(t("gui.records_failed", count=len(failed_dl) + len(failed_ids)))

        except SystemExit:
            show_error(t("gui.aborted_endpoint"))
        except Exception as e:
            show_error(t("gui.error", error=e))
        finally:
            start_btn.enable()
            spinner.classes(add="hidden")

    b_url.on("blur", fetch_endpoint_info)
    fetch_btn.on("click", fetch_endpoint_info)
    listsize_btn.on_click(check_list_size)
    start_btn.on_click(start_harvesting)


def start_gui():
    ui.run(title="Ometha", port=8765, reload=False)