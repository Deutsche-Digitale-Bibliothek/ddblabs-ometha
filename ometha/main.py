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
import urllib3
import yaml
from colorama import Fore, Style, init
from halo import Halo
from loguru import logger
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, HTTPError, Timeout

from ._version import __version__
from .cli import parseargs
from .harvester import change_date, create_id_file, get_identifier, harvest_files
from .helpers import (
    ACHTUNG,
    FEHLER,
    INFO,
    SEP_LINE,
    TIMESTR,
    log_critical_and_print_and_exit,
    print_and_log,
)
from .tui import interactiveMode


def start_process():
    multiprocessing.freeze_support()  # multiprocessing Einstellung
    multiprocessing.set_start_method("fork") if sys.platform == "darwin" else None
    init(autoreset=True)  # Colorama Einstellung:

    if getattr(sys, "frozen", False):
        application_path = os.path.dirname(os.path.abspath(sys.executable))
        running_mode = "Frozen/executable"
    else:
        try:
            application_path = os.path.dirname(os.path.abspath(__file__))
            running_mode = "Non-interactive"
        except NameError:
            application_path = os.getcwd()
            running_mode = "Interactive"

    config_path = (
        os.path.join(os.path.expanduser("~"), ".ometha")
        if sys.platform.startswith(("linux", "darwin"))
        else os.path.join(application_path, "ometha.yaml")
    )
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as yaml_configfile:
            headers = yaml.safe_load(yaml_configfile)
    else:
        if (
            input(
                "Konfigurationsdatei mit abweichenden Werten für den http-Header erstellen? y/N: "
            ).lower()
            == "y"
        ):
            ua = input("Wie soll der User-Agent String lauten?: ")
            frm = input(
                "Soll es 'from'-Feld im Header geben? (Leerlassen, wenn nein): "
            )
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

    # Session konfigurieren
    assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
    adapter = HTTPAdapter(
        max_retries=urllib3.util.retry.Retry(
            total=4, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=2
        )
    )
    session = requests.Session()
    session.hooks["response"].append(assert_status_hook)
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
    if os.getenv("OMETHA_DEBUG") == "True":
        print(PRM)
    # Ordner für Log, Configfile und Output anlegen im aktuellen Verzeichnis
    if PRM["out_f"] is None:
        PRM["out_f"] = os.path.join(os.getcwd(), "output")
    folder = os.path.join(PRM["out_f"], PRM["dat_geb"], TIMESTR)
    os.makedirs(folder, exist_ok=True)

    # Logfile anlegen
    logger.remove()  # Initalen Logger löschen, damit er nicht alles in stderr loggt:
    log_file = os.path.join(folder, f"_ometha_{PRM['dat_geb']}.log")
    logger.level("PARAMETER", no=38, color="<blue>")
    logger.add(
        log_file,
        level=0,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        enqueue=True,
    )
    parameters = {
        "Ometha Version": __version__,
        # fix: PRM["mode"] is not defined
        "Mode": f"{running_mode} with {PRM['mode']} Mode",
        "Datengeber": PRM["dat_geb"],
        "Base-URL": PRM["b_url"],
        "metadataPrefix": PRM["pref"],
        "Sets": PRM["sets"],
        "Timeout": PRM["timeout"],
        "Outputfolder": PRM["out_f"],
        "Count of parallel downloads": PRM["n_procs"],
    }
    for param, value in parameters.items():
        logger.log("PARAMETER", f"{param}: {value}")
    print(f"{INFO}Logfile: {log_file}")

    # bei Configmode & Automode das Datum der Konfigurationsdatei aktualisieren
    change_date(TIMESTR, PRM["conf_f"], key="until-Datum")

    # Baseurl anpassen/überprüfen
    PRM["b_url"] = re.sub(r"(\?.+)", "", PRM["b_url"]).rstrip("/")
    try:
        # try baseurl
        with Halo(text=f"Checking if {PRM['b_url']} is accessible", spinner="dots"):
            session.get(PRM["b_url"], verify=False, timeout=(20, 80))
    except (
        requests.exceptions.HTTPError,
        requests.exceptions.RetryError,
        urllib3.exceptions.MaxRetryError,
    ):
        print(f"{FEHLER} {PRM['b_url']} is not accessible. Trying with verb=Identify.")
        try:
            # try baseurl with verb Identify
            with Halo(
                text=f"Checking if {PRM['b_url']}?verb=Identify is accessible",
                spinner="dots",
            ):
                session.get(
                    PRM["b_url"] + "?verb=Identify", verify=False, timeout=(20, 80)
                )
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

    # scraping wenn in der commandline kein resumptiontoken angegeben ist
    def generate_id_harvesting_url(PRM, set):
        urlpar = {"from": "f_date", "until": "u_date", "metadataPrefix": "pref"}
        base_url = f"{PRM['b_url']}?verb=ListIdentifiers&"
        if PRM["res_tok"]:
            url = f"{base_url}&resumptionToken={PRM['res_tok']}"
            logger.info(
                f"Fortsetzen des Identifier-Harvestings bei: {re.sub('/$', '', url)}"
            )
        else:
            url = f"{base_url}{'&'.join(f'{name}={PRM[key]}' for name, key in urlpar.items() if PRM[key] is not None)}"
        if set:
            url = f"{url}&set={set}"

        return get_identifier(PRM, url, session)

    if PRM["sets"] is not None:
        # Initialize lists for comma and slash sets
        a_sets = PRM.get("sets", {})[0].get("additive", [])
        i_sets = PRM.get("sets", {})[0].get("intersection", [])
        # Initialize lists for comma and slash ids
        a_ids = [
            id for a_set in a_sets for id in generate_id_harvesting_url(PRM, a_set)
        ]
        i_ids = [
            id for i_set in i_sets for id in generate_id_harvesting_url(PRM, i_set)
        ]
        # If both i_ids and a_ids exist, get the common ids
        ids = list(set(i_ids) & set(a_ids)) if i_ids else a_ids
    else:
        ids = generate_id_harvesting_url(PRM, set=None)

    create_id_file(PRM, ids, folder, type="successful")
    # BUG If read from a yaml config file n_procs is a list with two values instead of an int
    # Dateiharvesting beginnen
    PRM["n_procs"] = (
        min(int(2 * len(ids) / 300 + 4), 16)
        if PRM["n_procs"] == 16
        else min(PRM["n_procs"], 100)
    )
    logger.info(
        f"Anzahl der parallelen Downloads auf {PRM['n_procs']} gesetzt (auf Basis der Anzahl an IDs)."
    )
    print(
        f"{Style.DIM} Timeout auf {PRM['timeout']} s gesetzt. {Style.RESET_ALL}\n{SEP_LINE}"
    ) if PRM["debug"] else None
    if len(ids) == 0:
        log_critical_and_print_and_exit(
            f"{SEP_LINE}{FEHLER} Keine IDs gefunden. Programm beendet."
        )
        sys.exit()
    failed_download, failed_ids = harvest_files(ids, PRM, folder, session)
    if failed_ids or failed_download:
        # failed_ids: Beim Harvesting übersprungene Dateien (wegen Timeout/Connection-problem). Hier nochmal versuchen!
        print(f"Fehlgeschlagene IDs: {failed_ids}") if PRM["debug"] else None
        failed_idfile = create_id_file(PRM, failed_ids, folder, type="failed")
        print_and_log(
            f"{SEP_LINE}{ACHTUNG} Es wurden IDs aufgrund von Verbindungsproblemen übersprungen.\n  Siehe dazu {failed_idfile}{SEP_LINE}{INFO} fehlgeschlagene IDs erneut harvesten..",
            logger,
            "warning",
        )
        PRM["n_procs"] = min(int(1 / (len(failed_ids) / len(ids)) + 1), 6)
        failed_download, failed_ids = harvest_files(failed_ids, PRM, folder, session)
        if failed_ids or failed_download:
            print_and_log(
                f"{SEP_LINE}{INFO} Harvesting von {len(failed_download) + len(failed_ids)} Datensätzen fehlgeschlagen\nProgramm beendet",
                logger,
                "info",
            )

        # print runtime
        runtime = "{:02}:{:02}:{:.3f}".format(
            *divmod(timeit.default_timer() - start_time, 3600, 60)
        )
        print_and_log(
            f"{SEP_LINE}{INFO} Vorgang hat {runtime} gedauert", logger, "info"
        )

    # bei Configmode & Automode das Datum der Konfigurationsdatei aktualisieren
    change_date(TIMESTR, PRM["conf_f"], key="from-Datum")

    if os.name == "nt":  # Beenden unter Windows
        input(f"{SEP_LINE}Drücken Sie Enter zum Beenden...")
    sys.exit()


if __name__ == "__main__":
    start_process()
