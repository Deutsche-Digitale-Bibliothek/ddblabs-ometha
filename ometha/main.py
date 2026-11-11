# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------
# Ometha
# ---------------------------------------------------------------------
import multiprocessing
import os
import re
import sys
import timeit

import requests
from loguru import logger
import urllib3
import yaml
from colorama import Fore, Style, init
from yaspin import yaspin
from yaspin.spinners import Spinners
from requests.adapters import HTTPAdapter

from ._version import __version__
from .cli import parseargs
from .harvester import (
    change_date,
    _split_sets,
    create_id_file,
    get_identifier_pages,
    harvest_files,
    read_yaml_file,
    write_summary_xml,
)
from .helpers import (
    ACHTUNG,
    FEHLER,
    INFO,
    OAITIMESTR,
    SEP_LINE,
    TIMESTR,
    configure_logging,
    log_critical_and_print_and_exit,
    print_and_log,
    safe_get,
)
from .tui import interactiveMode


def default_n_procs(PRM: dict, n_ids: int | None) -> int:
    """Anzahl paralleler Downloads: expliziter Wert (max. 100), sonst anhand der
    ID-Anzahl skaliert (max. 16). Bei Pipelining ist die Anzahl unbekannt → 16."""
    if PRM["n_procs"] is not None:
        return min(int(PRM["n_procs"]), 100)
    return 16 if n_ids is None else min(int(2 * n_ids / 300 + 4), 16)


def _has_time(d) -> bool:
    return bool(d) and "T" in str(d)


def generate_id_harvesting_url(
    PRM: dict, set: str, session: requests.Session, on_list_size=None
) -> list[str]:
    """Build a ListIdentifiers URL from PRM parameters and return all harvested IDs.

    If a resumption token is present in PRM it is used directly; otherwise the URL
    is assembled from the from/until dates, metadata prefix, and optional set.

    Args:
        PRM: Parameters dictionary with harvesting configuration.
        set: OAI set spec to restrict harvesting, or empty string for no set filter.
        session: HTTP session to use for requests.
        on_list_size: Optional callback invoked once completeListSize is known,
            used to feed the progress bar's ETA during pipelining.

    Returns:
        A list of all OAI identifier strings found at the endpoint.
    """
    urlpar = {"from": "f_date", "until": "u_date", "metadataPrefix": "pref"}
    base_url = f"{PRM['b_url']}?verb=ListIdentifiers"

    # OAI-PMH-Spec §3.3: from und until müssen dieselbe Granularität haben.
    # Enthält einer der Werte eine Uhrzeit (T…Z), beide auf Datetime-Granularität
    # hochstufen; enthält keiner eine Uhrzeit, beide auf Datums-Granularität
    # (YYYY-MM-DD) kürzen.
    f, u = PRM.get("f_date"), PRM.get("u_date")
    if f or u:
        if _has_time(f) or _has_time(u):
            # Datum ohne Zeit → Mitternacht UTC anhängen
            if f and not _has_time(f):
                PRM["f_date"] = f"{f}T00:00:00Z"
            if u and not _has_time(u):
                PRM["u_date"] = f"{u}T23:59:59Z"
        else:
            # Zeit abschneiden falls doch eine drin ist (defensiv)
            if f:
                PRM["f_date"] = str(f)[:10]
            if u:
                PRM["u_date"] = str(u)[:10]
        if f != PRM["f_date"] or u != PRM["u_date"]:
            logger.info(f"Datumsgranularität vereinheitlicht: from={PRM['f_date']} until={PRM['u_date']}")

    if PRM["res_tok"]:
        url = f"{base_url}&resumptionToken={PRM['res_tok']}"
        logger.info(f"Fortsetzen des Identifier-Harvestings bei: {re.sub('/$', '', url)}")
    else:
        params = "&".join(f"{name}={PRM[key]}" for name, key in urlpar.items() if PRM[key] is not None)
        url = f"{base_url}&{params}" if params else base_url
    if set:
        url = f"{url}&set={set}"

    return get_identifier_pages(PRM, url, session, on_list_size=on_list_size)


def collect_ids(PRM: dict, set: str | None, session: requests.Session, on_list_size=None):
    """Sammelt IDs für ein Set. Im Gentle Mode wird vollständig eingesammelt
    (kein Pipelining), sonst wird der Generator zum Pipelining durchgereicht."""
    if PRM["gentle"]:
        return [id for page in generate_id_harvesting_url(PRM, set, session) for id in page]
    return generate_id_harvesting_url(PRM, set, session, on_list_size=on_list_size)


def start_process() -> None:
    """Initialize and run the full Ometha harvesting pipeline.

    Handles multiprocessing setup, colorama initialization, HTTP session
    configuration, argument parsing (CLI or interactive), folder/log creation,
    identifier harvesting, and file harvesting with retry logic.
    """
    multiprocessing.freeze_support()  # multiprocessing Einstellung
    if sys.platform == "darwin" and multiprocessing.get_start_method(allow_none=True) is None:
        multiprocessing.set_start_method("fork")
    init(autoreset=True)  # Colorama Einstellung:

    if getattr(sys, "frozen", False):
        application_path = os.path.dirname(os.path.abspath(sys.executable))
    else:
        try:
            application_path = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            application_path = os.getcwd()

    config_path = (
        os.path.join(os.path.expanduser("~"), ".ometha")
        if sys.platform.startswith(("linux", "darwin"))
        else os.path.join(application_path, "ometha.yaml")
    )
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as yaml_configfile:
            headers = yaml.safe_load(yaml_configfile)
    else:
        if input("Konfigurationsdatei mit abweichenden Werten für den http-Header erstellen? y/N: ").lower() == "y":
            ua = input("Wie soll der User-Agent String lauten?: ")
            frm = input("Soll es 'from'-Feld im Header geben? (Leerlassen, wenn nein): ")
            headers = {"User-Agent": ua, "From": frm, "asciiart": True}
            with open(os.path.join(config_path), "w", encoding="utf8") as f:
                f.write(yaml.dump(headers))
        else:
            headers = {
                "User-Agent": f"Ometha {__version__}",
                "From": "test",
                "asciiart": True,
            }
            with open(os.path.join(config_path), "w", encoding="utf8") as f:
                f.write(yaml.dump(headers))
    if headers.get("asciiart", True):
        print(
            Fore.MAGENTA
            + """
    ██████╗ ███╗   ███╗███████╗████████╗██╗  ██╗ █████╗
    ██╔═══██╗████╗ ████║██╔════╝╚══██╔══╝██║  ██║██╔══██╗
    ██║   ██║██╔████╔██║█████╗     ██║   ███████║███████║
    ██║   ██║██║╚██╔╝██║██╔══╝     ██║   ██╔══██║██╔══██║
    ╚██████╔╝██║ ╚═╝ ██║███████╗   ██║   ██║  ██║██║  ██║
    ╚═════╝ ╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝
        """,
            f"{Style.DIM}Version {__version__}\n{SEP_LINE}",
        )
    headers = {"User-Agent": headers["User-Agent"], "From": headers["From"]}

    # Session konfigurieren – kein response-Hook, damit der HTTPAdapter-Retry
    # bei 429/5xx greifen kann ohne sofort eine HTTPError zu werfen.
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
    session.headers.update(headers)
    # disable warning for making http requests
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Argumente abfragen; Kommandozeile oder Interaktiv? bei interaktiv ohne automode und configfile
    PRM = parseargs() if len(sys.argv) > 1 else interactiveMode(session)

    # check that PRM is not None
    if not PRM:
        logger.critical("No parameters were passed to Ometha.")
        sys.exit()
    # output PRM dictionary if debug mode is enabled via environment variable
    if os.getenv("OMETHA_DEBUG") == "True":
        print(PRM)
    safe_timestr = TIMESTR.replace(":", "_")
    folder = os.path.join(PRM["out_f"], PRM["dat_geb"], safe_timestr)
    os.makedirs(folder, exist_ok=True)

    # bei Configmode & Automode das until-Datum setzen bevor es geloggt wird
    if PRM["conf_m"] and PRM["auto_m"]:
        change_date(OAITIMESTR, PRM["conf_f"], key="untildate")
        PRM["u_date"] = OAITIMESTR

    # Logfile anlegen
    logger.remove()  # Initalen Logger löschen, damit er nicht alles in stderr loggt:
    logger.level("PARAMETER", no=38, color="<blue>")
    if not PRM.get("no_log"):
        log_file = os.path.join(folder, f"_ometha_{PRM['dat_geb']}.log")
        logger.add(
            log_file,
            level=0,
            format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
            enqueue=True,
        )
        parameters = {
            "Ometha Version": __version__,
            "Datengeber": PRM["dat_geb"],
            "Base-URL": PRM["b_url"],
            "metadataPrefix": PRM["pref"],
        }
        if PRM["sets"]:
            parameters["Sets"] = PRM["sets"]
        if PRM["timeout"]:
            parameters["Timeout"] = PRM["timeout"]
        if PRM["out_f"]:
            parameters["Outputfolder"] = PRM["out_f"]
        if PRM["n_procs"] is not None:
            parameters["Number of parallel downloads"] = PRM["n_procs"]
        if PRM["f_date"] is not None:
            parameters["From date"] = PRM["f_date"]
        if PRM["u_date"] is not None:
            parameters["Until date"] = PRM["u_date"]
        for param, value in parameters.items():
            logger.log("PARAMETER", f"{param}: {value}")
        if PRM["id_f"] is not None:
            logger.log("PARAMETER", f"ID file: {PRM['id_f']}")
        print(f"{INFO}Logfile: {log_file}")

    # Baseurl anpassen/überprüfen
    PRM["b_url"] = re.sub(r"(\?.+)", "", PRM["b_url"]).rstrip("/")
    try:
        # try baseurl
        with yaspin(Spinners.dots, text=f"Checking if {PRM['b_url']} is accessible"):
            safe_get(session, PRM["b_url"], timeout=(20, 80))
    except (
        requests.exceptions.HTTPError,
        requests.exceptions.RetryError,
        urllib3.exceptions.MaxRetryError,
    ):
        print(f"{FEHLER} {PRM['b_url']} is not accessible. Trying with verb=Identify.")
        try:
            # try baseurl with verb Identify
            with yaspin(
                Spinners.dots,
                text=f"Checking if {PRM['b_url']}?verb=Identify is accessible",
            ):
                safe_get(session, PRM["b_url"] + "?verb=Identify", timeout=(20, 80))
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.RetryError,
            urllib3.exceptions.MaxRetryError,
        ) as e:
            # if both fail, log error and exit
            log_critical_and_print_and_exit(f"{FEHLER}{e}", PRM["mode"])
            sys.exit()

    # --------------------------------------------------------------------
    # Scraping
    # --------------------------------------------------------------------
    # Timer starten
    start_time = timeit.default_timer()

    # Wird gefüllt sobald completeListSize bekannt ist (nur bei Pipelining relevant),
    # damit der Fortschrittsbalken eine Gesamtzahl/ETA anzeigen kann.
    total_holder: list = [None]

    def _on_list_size(n: int) -> None:
        total_holder[0] = n

    def all_ids(set_spec):
        return [i for page in generate_id_harvesting_url(PRM, set_spec, session) for i in page]

    a_sets, i_sets = _split_sets(PRM)
    if PRM["id_f"] is not None:
        print(f"{INFO} IDs werden aus {PRM['id_f']} gelesen.")
        ids = read_yaml_file(PRM["id_f"], ["ids"])[0]
    elif i_sets:
        a_ids = [i for a_set in a_sets for i in all_ids(a_set)]
        i_ids = [i for i_set in i_sets for i in all_ids(i_set)]
        ids = list(set(i_ids) & set(a_ids)) if i_ids else a_ids
    elif len(a_sets) > 1:
        ids = list(dict.fromkeys(i for a_set in a_sets for i in all_ids(a_set)))
    else:
        # Ein oder kein Set: Pipelining (außer im Gentle Mode)
        ids = collect_ids(PRM, a_sets[0] if a_sets else None, session, on_list_size=_on_list_size)

    def handle_no_ids() -> None:
        if PRM.get("cleanup_empty"):
            import shutil

            shutil.rmtree(folder, ignore_errors=True)
        print_and_log(f"{SEP_LINE}Keine IDs gefunden. Programm beendet.", logger, "warning")
        change_date(OAITIMESTR, PRM["conf_f"], key="fromdate")
        sys.exit()

    pipelined = not isinstance(ids, list)
    collected: list[str] = []
    if pipelined:
        # IDs beim Durchreichen mitschreiben, damit die ID-Datei auch beim
        # Pipelining nach dem Harvest geschrieben werden kann
        def _tee(pages):
            for page in pages:
                collected.extend(page)
                yield page

        ids = _tee(ids)
    else:
        if not ids:
            handle_no_ids()
        create_id_file(PRM, ids, folder, type="successful")

    PRM["n_procs"] = default_n_procs(PRM, None if pipelined else len(ids))
    logger.info(f"Anzahl der parallelen Downloads auf {PRM['n_procs']} gesetzt (auf Basis der Anzahl an IDs).")
    print(f"{Style.DIM} Timeout auf {PRM['timeout']} s gesetzt. {Style.RESET_ALL}\n{SEP_LINE}") if PRM[
        "debug"
    ] else None
    # ---- Start Harvesting ----
    stats: dict = {}
    failed_download, failed_ids = harvest_files(
        ids, PRM, folder, session, total_holder=total_holder, stats=stats
    )
    if pipelined:
        if not collected:
            handle_no_ids()
        create_id_file(PRM, collected, folder, type="successful")

    # ---- Einmaliger Retry für alle fehlgeschlagenen IDs ----
    # failed_ids:       Timeout/Connection-Fehler → einmal wiederholen
    # failed_download:  HTTP-Fehler oder ungültige Antwort → auch wiederholen,
    #                   da manche Schnittstellen Datensätze erst ad hoc erzeugen
    retry_ids = list(set(failed_ids + failed_download))
    if retry_ids:
        print(f"Fehlgeschlagene IDs: {retry_ids}") if PRM["debug"] else None
        failed_idfile = create_id_file(PRM, retry_ids, folder, type="failed")
        print_and_log(
            f"{SEP_LINE}{ACHTUNG} {len(retry_ids)} IDs konnten nicht geharvestet werden.\n"
            f"  Siehe dazu {failed_idfile}\n"
            f"{SEP_LINE}{INFO} Fehlgeschlagene IDs werden einmalig erneut versucht...",
            logger,
            "warning",
        )
        # Weniger parallele Worker für den Retry – schont langsame Schnittstellen
        retry_n_procs = min(max(1, PRM["n_procs"] // 2), 6) if PRM["n_procs"] else 4
        PRM_retry = {**PRM, "n_procs": retry_n_procs}
        retry_stats: dict = {}
        failed_download, failed_ids = harvest_files(retry_ids, PRM_retry, folder, session, stats=retry_stats)
        # Zusammenfassung an den finalen Stand nach dem Retry anpassen:
        # ursprünglich fehlgeschlagene IDs, die beim Retry doch erfolgreich waren,
        # zählen jetzt als erfolgreich; die verbleibenden als endgültig fehlgeschlagen.
        stats["successful"] = stats.get("successful", 0) + retry_stats.get("successful", 0)
        stats["skipped_deleted"] = stats.get("skipped_deleted", 0) + retry_stats.get("skipped_deleted", 0)
        stats["moved_deleted"] = stats.get("moved_deleted", 0) + retry_stats.get("moved_deleted", 0)
        stats["moved_invalid"] = stats.get("moved_invalid", 0) + retry_stats.get("moved_invalid", 0)
        stats["failed_download"] = len(failed_download)
        stats["failed_ids"] = len(failed_ids)
        if failed_ids or failed_download:
            print_and_log(
                f"{SEP_LINE}{INFO} Nach erneutem Versuch: "
                f"{len(failed_download) + len(failed_ids)} Datensätze endgültig fehlgeschlagen.",
                logger,
                "warning",
            )
            create_id_file(PRM, failed_ids + failed_download, folder, type="final_failed")
        else:
            print_and_log(
                f"{SEP_LINE}{INFO} Alle IDs beim zweiten Versuch erfolgreich geharvestet.",
                logger,
                "info",
            )

    # print runtime
    elapsed = timeit.default_timer() - start_time
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    runtime = f"{int(hours):02}:{int(minutes):02}:{seconds:.3f}"
    print_and_log(f"{SEP_LINE}{INFO} Vorgang hat {runtime} gedauert", logger, "info")
    if PRM.get("summary"):
        summary_file = write_summary_xml(PRM, folder, stats, runtime)
        print_and_log(f"{INFO} Zusammenfassung: {summary_file}", logger, "info")

    change_date(OAITIMESTR, PRM["conf_f"], key="fromdate")

    if os.name == "nt":
        # Beenden unter Windows
        input(f"{SEP_LINE}Drücken Sie Enter zum Beenden...")
    sys.exit()


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "gui":
        from .gui import start_gui
        start_gui()
        return
    try:
        start_process()
    except KeyboardInterrupt:
        print_and_log(f"\n{ACHTUNG}Harvesting durch Nutzer abgebrochen.", logger, "warning")
        logger.complete()  # Log-Queue (enqueue=True) leeren, bevor hart beendet wird
        # os._exit statt sys.exit: sonst wartet Python beim Beenden auf die noch
        # laufenden Download-Threads (Timeouts/Retries) und hängt
        os._exit(0)


if __name__ == "__main__":
    logger = configure_logging()
    main()