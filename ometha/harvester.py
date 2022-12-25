import os
import re
import sys
import time
import urllib.parse
from collections import defaultdict
import tqdm
import asyncio
import aiohttp
import requests
import yaml
from halo import Halo
from colorama import Fore, Style
from loguru import logger
from lxml import etree
import urllib3
import platform
from p_tqdm import p_map
from loguru import logger
from functools import partial
from pathlib import Path
from urllib.parse import urlparse, parse_qs

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --------------------------------------------------------------------
# Funktionen
# --------------------------------------------------------------------


def getListSize(url, debug: bool, mode, session, multiplesets: bool) -> int:
    """Returns listSize as integer"""

    def readErrors(response):
        # Fehlermeldung der Schnittstelle auslesen
        if errors := (re.findall(r'error\scode="(.+)">(.+)<\/error', response.text)):
            print(
                f"{Fore.RED}Fehlermeldung: {Style.DIM}{errors[0][0]}/{errors[0][1]}{Style.RESET_ALL}"
            )
            logger.error(
                f"Fehlermeldung der Schnittstelle: {errors[0][0]}/{errors[0][1]}"
            )
            return errors

    # Verbindungsversuch inkl. Errorhandling um Schnittstellen Error und ListSize auszulesen
    try:
        response = session.get(url, verify=False, timeout=(20, 80))
        try:
            response.headers["Content-Type"]
        except:
            pass
        else:
            if "xml" not in response.headers["Content-Type"]:
                logger.error(
                    f"Bestimmung der ListSize: Wahrscheinlich kein valides XML im Return der Schnittstelle ({url}) (Content-Type ohne XML)"
                )
    except requests.exceptions.Timeout:
        if mode == "ui":
            input(
                "--------------------------------------\nLeider gab es einen Timeout bei der Bestimmung der ListSize. Die Schnittstelle ist nicht erreichbar.\nDrücken Sie Enter zum Beenden..."
            )
            if input != "":
                logger.critical(
                    "Timeout bei der Bestimmung der ListSize. Die Schnittstelle ist nicht erreichbar."
                )
                sys.exit()
        else:
            logger.critical(
                "Timeout bei der Bestimmung der ListSize. Die Schnittstelle ist nicht erreichbar."
            )
            sys.exit()
    except requests.exceptions.HTTPError as errh:
        if debug == True:
            print(f"{Fore.RED}Fehlermeldung:\n   {Style.DIM}{str(errh)}")
        if mode == "ui":
            input(
                "--------------------------------------\nLeider gab es einen HTTP Fehler.\nDrücken Sie Enter zum Beenden..."
            )
            if input != "":
                sys.exit()
        else:
            logger.critical(f"HTTP Fehler: {str(errh)}")
            sys.exit()
    except requests.exceptions.ConnectionError as errc:
        if debug == True:
            print(f"Fehlermeldung:\n  {(errc)}")
        if mode == "ui":
            input(
                f"--------------------------------------\n{Fore.RED}Leider war wiederholt keine Verbindung zu {url} möglich. Starten Sie den Harvester ggf. neu.{Fore.WHITE}\nDrücken Sie Enter zum Beenden..."
            )
            if input != "":
                sys.exit()
        else:
            sys.exit()
    else:
        if response.status_code == 404:
            if mode == "ui":
                input(
                    "--------------------------------------\nDie Schnitstelle ist nicht erreichbar.\nIst die URL korrekt?\nDrücken Sie Enter zum Beenden..."
                )
                if input != "":
                    sys.exit()
            else:
                logger.critical("Die Schnitstelle ist nicht erreichbar.")
                sys.exit()
        else:
            if readErrors(response):
                if mode == "ui":
                    input(
                        "--------------------------------------\nDie Schnittstelle hat einen Fehler gemeldet. Sind alle Parameter korrekt?\nDrücken Sie Enter zum Beenden..."
                    )
                    if input != "":
                        sys.exit()
                else:
                    logger.critical("Die Schnittstelle hat einen Fehler gemeldet")
                    if multiplesets == False:
                        sys.exit()
                    else:
                        logger.info(
                            f"{url} liefert keine Identifier, versuche das nächste Set."
                        )
                        return 0
            listSize = re.findall(r"completeListSize=[\"|'](\d+)[\"|']", response.text)
            try:
                listSize[0]
            except:
                # Keine ListSize gefunden
                listSize = "unbekannt"
            else:
                listSize = int(listSize[0])
            logger.info(f"Angegebene ListSize: {str(listSize)}")
            print(
                f"{Fore.GREEN}Information:\n   {Fore.WHITE}Angegebene ListSize: {str(listSize)}{Style.RESET_ALL}"
            )
            return listSize


def getIdentifier(
    baseurl: str,
    url,
    datengeber: str,
    folder,
    debug: bool,
    mprefix,
    listSize,
    mode,
    session,
):
    """
    Füllt das harvested_idfile mit den IDs der Schnittstelle. Iteriert über die ResumptionTokens.
    """
    spinner = Halo(text="Lade IDs...", spinner="dotes3")
    # URL bauen
    tokenbaseurl = baseurl + "?verb=ListIdentifiers&resumptionToken="
    # wir wollen die IDs sobald sie abgegriffen worden sind in eine Datei schreiben.
    # bei wiederholten ID harvestings am selben Tag kann es zwar sein, dass dann ganz viele zu viele IDs reinlaufen,
    # aber die fangen wir ja später bei der Deduplizierung wieder ab
    harvested_idfile = open(
        os.path.join(
            folder, time.strftime("%Y-%m-%d") + "_" + datengeber + "_ids.yaml"
        ),
        "a",
        encoding="utf8",
    )
    # Die Liste nutzen wir nur zum Anzeigen des Frotschritts.
    idlist = []

    def getResumptionToken(response) -> str:

        namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        # print(type(response))
        try:
            root = etree.XML(response.content)
        except etree.XMLSyntaxError as e:
            logger.error(f"Fehler beim Parsen des Resumption Tokens: '{e}'")
            token = None
        else:
            token = root.findall(f".//oai:resumptionToken", namespaces)
            try:
                token[0]
            except:
                token = None
            else:
                if token[0].text is not None:
                    token = token[0].text
                elif token[0].attrib["resumptionToken"] is not None:
                    token = token[0].attrib["resumptionToken"]
                else:
                    token = None
                # URL encode den resumptionToken (siehe https://gitlab.gwdg.de/maps/harvester/-/issues/25)
                urllib.parse.quote_plus(token)
                # print(f"{Style.DIM}Resumptiontoken: {Style.RESET_ALL}{token}")
        if debug:
            # TODO ist nicht schön in Kombination mit halo
            print(" Token: " + token)
        return token

    def getIDs(response):
        namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        try:
            root = etree.XML(response.content)
        except etree.XMLSyntaxError as e:
            logger.error(
                f"Fehler beim Parsen des Returns der OAI Identifier-Liste: '{e}'"
            )
        else:
            if root.findall(f".//oai:identifier", namespaces):
                for oaiid in root.findall(f".//oai:identifier", namespaces):
                    oaiid = oaiid.text
                    idlist.append(oaiid)
                    harvested_idfile.write("- '" + oaiid + "'\n")
            else:
                logger.warning(
                    f"Keine IDs (mehr) im Return der Schnittstelle ({url}) gefunden"
                )

    # 1. Ist die angegebene ListSize ermittelt? wenn nein, breche ab.
    try:
        listSize += 0
    except (TypeError, NameError):
        pass
    else:
        pass
        # print(f"{Fore.GREEN}Information: \n    Starte Harvesting von {(listSize)} Identifiern: {Fore.WHITE} \n--------------------------------------")

    # Jetzt kommt die eigentliche Schleife: Solange wir da nicht per break rausgehen, läuft die.

    spinner.start()
    while True:
        # Verbindungsversuch inkl. Errorhandling
        try:
            # Todo: use https://2.python-requests.org/en/master/user/quickstart/#passing-PARAMETERs-in-urls
            response = session.get(url, verify=False, timeout=(20, 80))
            try:
                response.headers["Content-Type"]
            except:
                pass
            else:
                if "xml" not in response.headers["Content-Type"]:
                    logger.error(
                        f"Abruf der OAI Identifier: Wahrscheinlich kein valides XML im Return der Schnittstelle ({url}) (Content-Type ohne XML)"
                    )
        except requests.exceptions.Timeout:
            logger.critical(f"Abbruch des Identifier-Harvestings wegen eines Timeouts.")
            try:
                token
            except:
                pass
            else:
                logger.info(
                    f"Starten Sie Ometha neu mit dem ResumptionToken {token} - zbsp.: 'Ometha default -b {baseurl} -m {mprefix} -d {datengeber} --resumptiontoken={token}'"
                )
            harvested_idfile.close()
            spinner.fail("Identifier Harvesting abgebrochen: Timeout")
            if mode == "ui":
                input(
                    "--------------------------------------\nLeider gab es einen Timeout.\nDrücken Sie Enter zum Beenden..."
                )
                if input != "":
                    sys.exit()
            else:
                sys.exit()
        except requests.exceptions.RetryError as errh:
            if debug == True:
                print(f"Fehlermeldung:\n   {errh}")
            logger.critical(
                f"Abbruch des Identifier-Harvestings wegen zu vieler Retries. Ist die Schnittstelle erreichbar?"
            )
            try:
                token
            except:
                pass
            else:
                logger.info(
                    f"Starten Sie Ometha neu mit dem ResumptionToken {token} - zbsp.: 'Ometha default -b {baseurl} -m {mprefix} -d {datengeber} --resumptiontoken={token}'"
                )
            harvested_idfile.close()
            spinner.fail(
                "Identifier Harvesting abgebrochen: Zu viele Retries. Ist die Schnittstelle erreichbar?"
            )
            if mode == "ui":
                input(
                    "--------------------------------------\nIdentifier Harvesting abgebrochen wegen zu vieler Retries.\nDrücken Sie Enter zum Beenden..."
                )
                if input != "":
                    sys.exit()
            else:
                sys.exit()
        except requests.exceptions.HTTPError as errh:
            if debug == True:
                print(f"Fehlermeldung:\n   {errh}")
            logger.critical(
                f"Abbruch des Identifier-Harvestings wegen eines Fehlers: {errh}."
            )
            try:
                token
            except:
                pass
            else:
                logger.info(
                    f"Starten Sie Ometha neu mit dem ResumptionToken {token} - zbsp.: 'Ometha default -b {baseurl} -m {mprefix} -d {datengeber} --resumptiontoken={token}'"
                )
            harvested_idfile.close()
            spinner.fail(f"Identifier Harvesting abgebrochen: {errh}")
            if mode == "ui":
                input(
                    "--------------------------------------\nLeider gab es einen HTTP Fehler.\nDrücken Sie Enter zum Beenden..."
                )
                if input != "":
                    sys.exit()
            else:
                sys.exit()
        except requests.exceptions.ConnectionError as errc:
            if debug == True:
                print(f"Fehlermeldung:\n   {errc}")
            try:
                token
            except:
                pass
            else:
                logger.info(
                    f"Starten Sie Ometha neu mit dem ResumptionToken {token} - zbsp.: 'Ometha default -b {baseurl} -m {mprefix} -d {datengeber} --resumptiontoken={token}'"
                )
            logger.critical(
                f"Abbruch des Identifier-Harvestings wegen eines Fehlers: {errc}."
            )
            harvested_idfile.close()
            spinner.fail(f"Identifier Harvesting abgebrochen: {errc}")
            # TODO Errorhandling, großzügige neue Versuche
            if mode == "ui":
                input(
                    f"--------------------------------------\n Leider war wiederholt keine Verbindung zu {url} möglich. Starten Sie den Harvester ggf. neu.\nDrücken Sie Enter zum Beenden..."
                )
                if input != "":
                    sys.exit()
            else:
                sys.exit()
        else:
            if response.status_code == 404:
                if mode == "ui":
                    input(
                        "--------------------------------------\nDie Schnitstelle ist nicht erreichbar.\nIst die URL korrekt?\nDrücken Sie Enter zum Beenden..."
                    )
                    if input != "":
                        sys.exit()  #
                else:
                    logger.critical("Die Schnitstelle ist nicht erreichbar.")
                    sys.exit()
            else:
                # Jetzt endlich die IDs holen aus der Response
                getIDs(response)
                spinner.text = str(len(idlist)) + " IDs geharvestet"
                # schauen, ob es noch einen Token gibt oder ob wir aus der Schleife rausmüssen:
                try:
                    token = getResumptionToken(response)
                    # Es gibt einen neuen ResumptionToken:
                    # neue URL zusammensetzen
                    url = tokenbaseurl + token
                except:
                    # wenn kein ResumptionToken in der response gefunden wird,
                    # erst nach Fehlermeldungen in der response gucken:
                    if errors := re.findall(
                        r"error\scode=['\"](.+)['\"]>(.*)<\/error", response.text
                    ):
                        spinner.fail()
                        logger.critical(
                            f"Fehler beim Harvesten der Identifier: {errors[0][0]}"
                        )
                        logger.info(f"Fehlerhafte URL: {url}")
                        if mode == "ui":
                            input(
                                f"{Fore.RED} Fehler beim Harvesten der Identifier: {Style.DIM} {errors[0][0]} {Style.RESET_ALL} \n--------------------------------------\nDrücken Sie Enter zum Beenden..."
                            )
                            if input != "":
                                sys.exit()
                        else:
                            sys.exit()
                    else:
                        # Es gibt keine Fehlermeldung und keinen weiteren ResumptionToken:
                        # wir sind fertig und gehen per break aus dem while-Loop raus.
                        # print(
                        #     f"\n--------------------------------------\n{Fore.GREEN}Information: \n    Identifier Harvesting beendet.")
                        spinner.succeed(
                            f"Identifier Harvesting beendet. Insgesamt {len(idlist)} IDs bekommen."
                        )
                        logger.info(f"Letzte abgefragte URL: {url}")
                        logger.info("Identifier Harvesting beendet.")
                        harvested_idfile.close()
                        break


def createurl(
    baseurl: str,
    mprefix: str,
    oaiset: str,
    fromdate: str,
    untildate: str,
    idfile,
    datengeber: str,
    debug: bool,
    folder,
    mode,
    session,
    multiplesets=False,
):
    """
    Erzeugt die URL
    Ruft getListSize und dann getIdentifier auf
    """
    # global harvested_idfile, listSize

    # harvested_idfile = idfile
    if oaiset is not None:
        oaiset = "&set=" + oaiset
    else:
        oaiset = ""
    if fromdate is not None:
        fromdate = "&from=" + str(fromdate)
    else:
        fromdate = ""
    if untildate is not None:
        untildate = "&until=" + str(untildate)
    else:
        untildate = ""
    url = (
        baseurl
        + "?verb=ListIdentifiers&"
        + "metadataPrefix="
        + mprefix
        + oaiset
        + fromdate
        + untildate
    )
    print(
        f"URL:{Style.DIM} {url}{Style.RESET_ALL} \n--------------------------------------"
    )
    logger.info(f"Abgefragte URL: {url}")
    listSize = getListSize(url, debug, mode, session, multiplesets)
    if listSize != 0:
        getIdentifier(
            baseurl, url, datengeber, folder, debug, mprefix, listSize, mode, session
        )
    else:
        logger.error("Keine Identifier")


def harvestfiles(
    ids: list,
    baseurl: str,
    mprefix: str,
    folder,
    timeout: float,
    mode: str,
    numberofprocesses: int,
    session: requests.Session,
) -> list:
    """
    Liest ID Liste und lädt die IDs einzeln über GetRecord.
    Returns failed_download, failed_ids ID-Lists
    """

    def savexml(oaiid: str, folder: str, response):
        fname = re.sub(r"([:.|&%$=()\"#+\'´`*~<>!?/;,\[\]]|\s)", "_", oaiid)
        with open(os.path.join(folder, fname + ".xml"), "w", encoding="utf8") as of:
            of.write(response)

    initial_length_of_ids = len(ids)

    if len(ids) < 1:
        if mode == "ui":
            input(
                f"--------------------------------------\n{Fore.RED}Keine Identifier bekommen, breche ab.{Fore.WHITE}\n--------------------------------------\nDrücken Sie Enter zum Beenden..."
            )
            if input != "":
                sys.exit()
        else:
            logger.critical("Keine Identifier bekommen, breche ab.")
            sys.exit()
    if initial_length_of_ids != len(ids):
        logger.info(
            f"Harveste {len(ids)} IDs nach Deduplizierung. ({initial_length_of_ids-len(ids)} Dubletten)"
        )
    else:
        logger.info(f"Harveste {len(ids)} IDs'.")

    print(
        f"--------------------------------------\nHarveste {len(ids)} Dateien\n--------------------------------------"
    )

    failed_oaiids = []

    def get_text(url, session):
        oaiid = parse_qs(urlparse(url).query)["identifier"][0]
        try:

            with session.get(url) as resp:
                if resp.status_code != 200:
                    logger.critical(f"Statuscode {resp.status_code} bei {url}")
                else:
                    xmlcontent = resp.text
                # Fehler rausfiltern:
                if errors := (
                    re.findall(r'error\scode="(.+)">(.+)<\/error', xmlcontent)
                ):
                    logger.warning(
                        f"Datei {url} konnte nicht geharvestet werden ('{errors[0][0]}')"
                    )
                    return {"failed_download": oaiid}
                else:
                    savexml(oaiid, folder, xmlcontent)
        except requests.exceptions.ConnectionError:
            logger.warning(
                f"Datei {url} wurde aufgrund eines Verbindungsproblems übersprungen"
            )
            return {"failed_id": oaiid}
        except requests.exceptions.Timeout:
            logger.warning(f"Datei {url} wurde aufgrund eines Timeouts übersprungen")
            return {"failed_id": oaiid}
        except requests.exceptions.RequestException as err:
            logger.warning(
                f"Datei {url} wurde aufgrund eines anderen Fehlers übersprungen: {err}"
            )
            return {"failed_id": oaiid}

    if numberofprocesses > 100:
        numberofprocesses = 100
        logger.warning("Anzahl paralleler Downloads auf 100 limitiert")

    urls = [
        baseurl + "?verb=GetRecord&metadataPrefix=" + mprefix + "&identifier=" + str(id)
        for id in ids
    ]

    failed_oaiids = p_map(
        partial(get_text, session=session),
        urls,
        total=len(urls),
        **{
            "num_cpus": numberofprocesses,
            "desc": "Harveste...",
            "unit": "files",
            "dynamic_ncols": True,
        },
    )

    failed_files = defaultdict(list)

    for subdict in failed_oaiids:
        if subdict is not None:
            for key in subdict:
                failed_files[key].append(subdict[key])

    failed_files = dict(failed_files)

    try:
        failed_files["failed_id"]
    except:
        failed_ids = []
    else:
        failed_ids = failed_files["failed_id"]
    try:
        failed_files["failed_download"]
    except:
        failed_download = []
    else:
        failed_download = failed_files["failed_download"]

    logger.info(f"Harvesting beendet")

    return failed_download, failed_ids


def change_f_date(fdate: str, fname: str):
    # Configdatei laden
    with open(fname) as f:
        doc = yaml.safe_load(f)
    # nach dem harvesten: fromdate updaten auf aktuelles Datum, damit der
    # nächste Vorgang dann ab da geht, wo der letzte harvestingvorgang gelaufen ist
    doc["fromdate"] = fdate
    with open(fname, "w") as f:
        yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)


def change_u_date(udate: str, fname: str):
    # Configdatei laden
    with open(fname) as f:
        doc = yaml.safe_load(f)
    # untildate vor dem harvesten auf heute setzen
    doc["untildate"] = udate
    with open(fname, "w") as f:
        yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)
