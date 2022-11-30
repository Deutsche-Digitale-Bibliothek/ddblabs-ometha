# -*- coding: utf-8 -*-
# ---------------------------------------------------------------------
# Ometha
# Karl Krägelin <kraegelin@sub.uni-goettingen.de>
# SUB Göttingen
# 2019-2022
# MIT License
# ---------------------------------------------------------------------
import multiprocessing
import os
import re
import sys
import time
import timeit
import colorama
import requests
import yaml
from halo import Halo
from colorama import AnsiToWin32, Fore, Style
from loguru import logger
from .cli import *
from .tui import *
from .harvester import *
import pkg_resources  # part of setuptools

# get version from setup.py
__version__ = pkg_resources.require("ometha")[0].version

# Initalen Logger löschen, damit er nicht alles in stderr loggt:
logger.remove()

TIMESTR = time.strftime("%Y-%m-%dT%H:%M:%SZ")


def setup_requests(requestheaders) -> requests.Session:
    """Sets up a requests session to automatically retry on errors

    cf. <https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/>

    Returns
    -------
    http : requests.Session
        A fully configured requests Session object
    """
    http = requests.Session()
    assert_status_hook = lambda response, *args, **kwargs: response.raise_for_status()
    http.hooks["response"] = [assert_status_hook]
    retry_strategy = Retry(
        total=4,  # Total number of retries to allow.
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],  # A set of HTTP status codes that we should force a retry on.
        method_whitelist=["GET"],
        backoff_factor=2,  # A backoff factor to apply between attempts: {backoff factor} * (2 ** ({number of total retries} - 1)) / 1, 2, 4 Sekunden
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    http.headers.update(requestheaders)
    return http


def main():
    multiprocessing.freeze_support()
    if sys.platform == "darwin":
        multiprocessing.set_start_method("fork")
    # Colorama Einstellung:
    colorama.init(autoreset=True)
    stream = AnsiToWin32(sys.stderr).stream
    print(
        Fore.MAGENTA
        + """
 ██████╗ ███╗   ███╗███████╗████████╗██╗  ██╗ █████╗
██╔═══██╗████╗ ████║██╔════╝╚══██╔══╝██║  ██║██╔══██╗
██║   ██║██╔████╔██║█████╗     ██║   ███████║███████║
██║   ██║██║╚██╔╝██║██╔══╝     ██║   ██╔══██║██╔══██║
╚██████╔╝██║ ╚═╝ ██║███████╗   ██║   ██║  ██║██║  ██║
 ╚═════╝ ╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝
    """
    )
    print(f"{Style.DIM}Version {__version__}\n--------------------------------------\n")

    requestheaders = {"User-Agent": "Ometha " + __version__}
    session = setup_requests(requestheaders)

    # --------------------------------------------------------------------
    # Läuft eine kompilierte Variante oder nicht?
    # --------------------------------------------------------------------

    if getattr(sys, "frozen", False):
        application_path = os.path.dirname(sys.executable)
        running_mode = "Frozen/executable"
    else:
        try:
            app_full_path = os.path.realpath(__file__)
            application_path = os.path.dirname(app_full_path)
            import pretty_errors

            running_mode = "Non-interactive"
        except NameError:
            # trigger nur wenn man in einer interaktiven session import ometha etc. nutzt
            application_path = os.getcwd()
            running_mode = "Interactive"

    # --------------------------------------------------------------------
    # Argumente abfragen; Kommandozeile oder Interaktiv?
    # --------------------------------------------------------------------

    if len(sys.argv) > 1:
        # wenn mindestens 1 KommandozeilenPARAMETER vergeben wurde, dann gehe in de parseargs Funktion -> Kommandozeilenmodus
        (
            baseurl,
            mprefix,
            datengeber,
            oaiset,
            debug,
            timeout,
            idfile,
            fromdate,
            untildate,
            resumptiontoken,
            configfile,
            configmode,
            automode,
            outputfolder,
            numberofprocesses,
        ) = cli.parseargs()
        folder = os.path.join(outputfolder, datengeber, time.strftime("%Y-%m-%d"))
        mode = "cli"

    else:
        # Wenn keine Kommandozeilenparamter angegeben sind, dann wechsle hin den interaktiven Modus
        (
            baseurl,
            mprefix,
            datengeber,
            oaiset,
            fromdate,
            untildate,
            idfile,
            resumptiontoken,
            timeout,
            debug,
            configmode,
            outputfolder,
            numberofprocesses,
        ) = tui.interactiveMode(session)
        folder = os.path.join(outputfolder, datengeber, time.strftime("%Y-%m-%d"))
        mode = "ui"

    # --------------------------------------------------------------------
    # Gibt es eine Konfiguration? Wenn ja, die laden.
    # --------------------------------------------------------------------

    if sys.platform.startswith("linux") or sys.platform.startswith("darwin"):
        configpath = os.path.join(os.path.expanduser("~"), ".ometha.yaml")
    else:
        configpath = os.path.join(application_path, "ometha.yaml")

    if os.path.exists(configpath):
        yaml_config_filepath = open(configpath, "r", encoding="utf-8")
        configfile = yaml.safe_load(yaml_config_filepath)
        try:
            requestheaders = configfile["requestheaders"][0]
        except:
            pass
        else:
            session = setup_requests(requestheaders)
    else:
        customConfig = input(
            "Wollen Sie eine Konfigurationsdatei mit abweichenden Werten für den HTTP Header erstellen? y/N: "
        )
        if customConfig != "y":
            yamlrequestheaders = {
                "requestheaders": [{"User-Agent": "Ometha " + __version__}]
            }
            with open(configpath, "w", encoding="utf8") as f:
                f.write(yaml.dump(yamlrequestheaders))
        else:
            ua = input("Wie soll der User-Agent String lauten?: ")
            frm = input(
                "Soll es ein 'From' Feld im Header geben? (Leerlassen, wenn nein, ansonsten ausfüllen): "
            )
            requestheaders = {"User-Agent": ua, "From": frm}
            yamlrequestheaders = {"requestheaders": [{"User-Agent": ua, "From": frm}]}
            with open(configpath, "w", encoding="utf8") as f:
                f.write(yaml.dump(yamlrequestheaders))
            session = setup_requests(requestheaders)

    # --------------------------------------------------------------------
    # Log beginnen
    # --------------------------------------------------------------------

    logname = os.path.join(
        folder, time.strftime("%Y-%m-%d_%H%M") + "_Ometha_" + datengeber + ".log"
    )
    PARAMETER = logger.level("PARAMETER", no=38, color="<blue>")
    logger.add(
        logname,
        level=0,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        enqueue=True,
    )
    logger.log("PARAMETER", f"Harvestingvorgang mit Ometha {__version__} gestartet")
    logger.log("PARAMETER", f"Modus: {running_mode} with {mode} Mode")
    logger.log("PARAMETER", f"Datengeber: {datengeber}")
    logger.log("PARAMETER", f"Base-URL: {baseurl}")
    logger.log("PARAMETER", f"metadataPrefix: {mprefix}")
    logger.log("PARAMETER", f"Sets: {oaiset}")
    logger.log("PARAMETER", f"Timeout: {timeout}")
    logger.log("PARAMETER", f"Outputfolder: {outputfolder}")
    logger.log("PARAMETER", f"Count of parallel downloads: {numberofprocesses}")

    # --------------------------------------------------------------------
    # Configmode & Automode
    # --------------------------------------------------------------------

    if configmode == True and automode == True:
        print(
            f"{Fore.YELLOW} -> Information: Setze until-Datum der Konfigurationsdatei {configfile} auf das aktuelle Datum ({time.strftime('%Y-%m-%d')})"
        )
        change_u_date(time.strftime("%Y-%m-%d"), configfile)
        # neues untildate setzen
        untildate = time.strftime("%Y-%m-%d")
        logger.info(
            f"until-Datum der YAML Konfigurationsdatei aktualisiert auf {time.strftime('%Y-%m-%d')}"
        )
    else:
        pass

    # --------------------------------------------------------------------
    # Ordner erstellen
    # --------------------------------------------------------------------

    if os.path.exists(folder):
        pass
    else:
        os.mkdir(folder)

    # --------------------------------------------------------------------
    # Baseurl anpassen/überprüfen
    # --------------------------------------------------------------------

    # baseurl Parameter löschen, falls fälschlicherweise übergeben
    baseurl = re.sub(r"(\?.+)", "", re.sub(r"\/$", "", baseurl))
    # baseurl überprüfen
    try:
        session.get(baseurl, verify=False, timeout=(20, 80))
    except (
        requests.exceptions.HTTPError,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
    ) as e:
        logger.critical(f"Fehlermeldung: Die Schnittstelle ist nicht erreichbar: {e}")
        if mode == "ui":
            input(
                f"--------------------------------------\n{Fore.RED}Fehlermeldung:\n  {Style.DIM}Die Schnittstelle ist nicht erreichbar.\n{Style.RESET_ALL}Drücken Sie Enter zum Beenden..."
            )
            if input != "":
                sys.exit()
        else:
            sys.exit()
    else:
        pass

    # --------------------------------------------------------------------
    # Fangen wir neu an oder mit ResumptionToken nach Abbruch?
    # Wenn ohne ResumptionToken, dann mit ID Datei oder ohne?
    # Normales Harvesting: createurl() -> getIdentifier()
    # mit ResumptionToken: getIdentifier()
    # mit ID-File: ID-File testen
    # dann in jedem Fall weiter zu harvestfiles(harvested_idfile)
    # --------------------------------------------------------------------
    start_time = timeit.default_timer()
    if resumptiontoken is None:
        if idfile is None:
            logger.log("PARAMETER", f"fromdate: {fromdate}")
            logger.log("PARAMETER", f"untildate: {untildate}")
            # wenn ein normales, neues Harvesting angestoßen wird

            harvested_idfile = os.path.join(
                folder, time.strftime("%Y-%m-%d") + "_" + datengeber + "_ids.yaml"
            )
            with open(harvested_idfile, "w") as f:
                f.write(
                    f"info: ID Liste erzeugt mit Ometha {__version__}\ndate: {TIMESTR}\nurl: {baseurl}\nset: {oaiset}\nmprefix: {mprefix}\nids:\n"
                )
            if debug == True:
                print(
                    f"{Style.DIM} Timeout auf {timeout} Sekunden gesetzt. {Style.RESET_ALL} \n--------------------------------------"
                )
            if oaiset:
                if len(oaiset) == 1:
                    createurl(
                        baseurl,
                        mprefix,
                        "".join(oaiset),
                        fromdate,
                        untildate,
                        harvested_idfile,
                        datengeber,
                        debug,
                        folder,
                        mode,
                        session,
                    )
                elif len(oaiset) > 1:
                    for setid in oaiset:
                        setid = re.sub(r"(^\s+|\s+$)", "", setid)
                        print(
                            f"--------------------------------------\nStarte Harvesting des Sets '{setid}'"
                        )
                        logger.info(f"Starte Harvesting des Sets '{setid}'")
                        createurl(
                            baseurl,
                            mprefix,
                            setid,
                            fromdate,
                            untildate,
                            harvested_idfile,
                            datengeber,
                            debug,
                            folder,
                            mode,
                            session,
                            multiplesets=True,
                        )
            else:
                createurl(
                    baseurl,
                    mprefix,
                    oaiset,
                    fromdate,
                    untildate,
                    harvested_idfile,
                    datengeber,
                    debug,
                    folder,
                    mode,
                    session,
                )
        else:
            # Wenn ein ID-File übergeben wurde mit -i
            logger.info("Harveste mit ID File")
            try:
                open(idfile, "r", encoding="utf-8")
            except:
                logger.critical(
                    "Fehlermeldung: Die ID-Datei kann leider nicht gelesen werden"
                )
                if mode == "ui":
                    input(
                        f"--------------------------------------\n{Fore.RED}Fehlermeldung:\n  {Style.DIM}Die ID-Datei kann leider nicht gelesen werden.\n{Style.RESET_ALL}Drücken Sie Enter zum Beenden..."
                    )
                    if input != "":
                        sys.exit()
                else:
                    sys.exit()
            else:
                harvested_idfile = idfile

    else:
        # wenn ein Resumption Token übergeben wurde
        urlWorking = session.get(baseurl, verify=False)
        if urlWorking.status_code == 404:
            if mode == "ui":
                input(
                    "--------------------------------------\nDie Schnittstelle ist nicht erreichbar.\nDrücken Sie Enter zum Beenden..."
                )
                if input != "":
                    sys.exit()
            else:
                logger.critical("Die Schnittstelle ist nicht erreichbar")
                sys.exit()
        print(
            f"--------------------------------------\nFortsetzen des Identifier-Harvestings bei: {re.sub('/$', '', baseurl)}?verb=ListIdentifiers&resumptionToken={resumptiontoken}"
        )
        logger.info(
            f"Fortsetzen des Identifier-Harvestings bei: {re.sub('/$', '', baseurl)}?verb=ListIdentifiers&resumptionToken={resumptiontoken}"
        )
        # Was passiert, wenn zwischendrin ein Tageswechsel war? Also Beginn am 1.1. und Abbruch am 2.1.?
        harvested_idfile = os.path.join(
            folder, time.strftime("%Y-%m-%d") + "_" + datengeber + "_ids.yaml"
        )
        # Identifier harvesten ab Resumptiontoken
        # Muss den Folder kennen damit die ID-Liste fortgesetzt werden kann
        getIdentifier(
            baseurl,
            baseurl + "?verb=ListIdentifiers&resumptionToken=" + resumptiontoken,
            datengeber,
            folder,
            debug,
            mprefix,
            0,
            mode,
        )

    # --------------------------------------------------------------------
    # File Harvesting starten
    # --------------------------------------------------------------------

    try:
        from yaml import CLoader as Loader
    except ImportError:
        from yaml import Loader
    try:
        with Halo(text='Lese YAML ID Liste...', spinner='dots'):
            yamllist = yaml.load(open(harvested_idfile, "r", encoding="utf-8"), Loader=Loader)
    except:
        pass
    else:
        print("YAML ID Liste erfolgreich geladen")
    oaiidentifier = set(yamllist["ids"])
    totalnumberofids = len(oaiidentifier)

    # Dateiharvesting beginnen

    if numberofprocesses == 16:
        # Triggert nur, wenn kein abweichender Wert für die Anzahl
        # an parallelen Downloads vergeben wurde.
        if totalnumberofids <= 200:
            numberofprocesses = 4
        elif totalnumberofids > 200 and totalnumberofids < 750:
            numberofprocesses = 8
        else:
            numberofprocesses = 16
        logger.info(
            f"Anzahl der parallelen Download Vorgänge auf {numberofprocesses} gesetzt auf Basis der Anzahl an IDs."
        )

    failed_download, failed_ids = harvestfiles(
        oaiidentifier,
        baseurl,
        mprefix,
        folder,
        timeout,
        mode,
        numberofprocesses,
        session
    )

    # --------------------------------------------------------------------

    # Differenz ListSize und IDs
    try:
        listSize += 0
    except (TypeError, NameError):
        pass
    else:
        if listSize - len(oaiidentifier) > 0:
            print(
                f"--------------------------------------\n{Fore.YELLOW}Achtung:\n {Fore.WHITE}Die angegebene ListSize ({listSize}) und die tatsächlich bekommenen Identifier ({len(oaiidentifier)}) weichen voneinander ab! {Fore.WHITE}"
            )
            logger.warning(
                f"Die angegebene ListSize({(listSize)}) und die tatsächlich bekommenen Identifier ({len(oaiidentifier)}) weichen voneinander ab."
            )
    if failed_ids:
        # failed_ids: Beim Harvesting übersprungene Dateien (wegen Timeout/Connectionproblem). Hier das Harvesten nochmal versuchen!
        failed_idfile_path = os.path.join(folder, datengeber + "_failed_ids.yml")
        if debug == True:
            print(f"Fehlgeschlagene IDs: {failed_ids}")
        with open(failed_idfile_path, "w", encoding="utf8") as failed_idfile:
            failed_idfile.write(
                f"info: Failed-ID Liste erzeugt mit Ometha {__version__}\ndate: {TIMESTR}\nurl: {baseurl}\nset: {oaiset}\nmprefix: {mprefix}\nids:\n"
            )
            for fid in failed_ids:
                failed_idfile.write(f"- '{fid}'\n")
        print(
            f"--------------------------------------\n{Fore.YELLOW}Achtung:\n  {len(failed_ids)} IDs wurden beim Harvesten auf Grund von Verbindungsproblemen übersprungen.\n  Siehe dazu auch '{failed_idfile_path}'.{Fore.WHITE}"
        )
        logger.warning(f"{len(failed_ids)} übersprungene IDs.")
        # --------------------------------------------------------------------
        # Die fehlgeschlagenen IDs nochmal harvesten
        # --------------------------------------------------------------------
        print(
            f"--------------------------------------\n{Fore.GREEN}Information:\n  Versuche, die fehlgeschlagenen IDs aus {failed_idfile_path} erneut zu harvesten.{Style.RESET_ALL}"
        )

        if len(failed_ids) / totalnumberofids > 0.5:
            # wenn mehr als 50% der IDs nicht geharvestet werden konnten
            numberofprocesses = 2
        elif len(failed_ids) / totalnumberofids > 0.2:
            # wenn mehr als 20% der IDs nicht geharvestet werden konnten
            numberofprocesses = 4
        else:
            numberofprocesses = 6

        logger.info(
            f"Starte Versuch, die fehlgeschlagenen IDs aus {failed_idfile_path} mit {numberofprocesses} parallelen Downloads erneut zu harvesten."
        )
        failed_download, failed_ids = harvestfiles(
            failed_idfile_path,
            baseurl,
            mprefix,
            folder,
            timeout,
            mode,
            numberofprocesses,
            session,
        )

    if failed_download:
        # failed_download: Angefragte IDs die eine Fehlermeldung des Servers enthalten (kein XML return)
        print(
            f"{Fore.YELLOW}Achtung: \n {len(failed_download)} Dateien waren auf der Schnittstelle nicht verfügbar. {Fore.WHITE}"
        )

    if failed_ids or failed_download:
        allfailed = len(failed_download) + len(failed_ids)
        print(
            f"--------------------------------------\n{Fore.YELLOW}Information: {Fore.WHITE}\nHarvesting von insgesamt {allfailed} Datensätzen fehlgeschlagen"
        )

    logger.info("Programm beendet.")

    executiontime = timeit.default_timer() - start_time
    if executiontime / 60 >= 60:
        printtime = str(round(executiontime / 60 / 60, 3)) + " Stunden"
    elif executiontime / 60 <= 2:
        printtime = str(round(executiontime, 3)) + " Sekunden"
    else:
        printtime = str(round(executiontime / 60, 3)) + " Minuten"
    print(
        f"--------------------------------------\n{Fore.GREEN}Information: {Fore.WHITE}\n   Der gesamte Vorgang hat ca. {printtime} gedauert"
    )
    logger.debug("Zeit: " + str(executiontime) + " Sekunden")

    print(f"Logfile: {logname}")

    if configmode == True and automode == True:
        print(
            f"---------------------------------------\n{Fore.YELLOW}-> Information: Setze das from-Datum der Konfigurationsdatei {configfile} auf das aktuelle Datum"
        )
        change_f_date(time.strftime("%Y-%m-%d"), configfile)

    # Beenden unter Windows...

    if os.name == "nt":
        inp = input(
            "--------------------------------------\nDrücken Sie Enter zum Beenden..."
        )
        if inp != "":
            sys.exit()
    else:
        sys.exit()


if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        print("Beende Ometha")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
