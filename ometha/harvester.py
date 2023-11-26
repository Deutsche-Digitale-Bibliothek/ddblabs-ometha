import sys
import time
import yaml
from halo import Halo
from lxml import etree
from builtins import input
from collections import defaultdict
from functools import partial
from urllib.parse import urlparse, parse_qs
import os
import re
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm
from loguru import logger
from colorama import Fore, Style
import json
import xmltodict
from requests.exceptions import HTTPError, ConnectionError, Timeout, RetryError, RequestException

# define global variables reused throughout the code
SEP_LINE = "--------------------------------------\n"
ACHTUNG = f"{Fore.YELLOW}Achtung:\n {Fore.WHITE}"
FEHLER = f"{Fore.RED}Fehler:\n  {Style.DIM}"
INFO = f"{Fore.YELLOW}Information: {Fore.WHITE}"
TIMESTR = time.strftime("%Y-%m-%dT%H:%M:%SZ")
NAMESPACE = "{http://www.openarchives.org/OAI/2.0/}"
ISODATEREGEX = "(?:19|20)[0-9]{2}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-9])|(?:(?!02)(?:0[1-9]|1[0-2])-(?:30))|(?:(?:0[13578]|1[02])-31))"
URLREGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
__version__ = '1.9.7'

# initialize all parameters in a dict shortened as PRM
PRM = {
    "b_url": None,      # base url: str
    "pref": None,       # metadata prefix: str
    "dat_geb": None,    # datengeber: int
    "sets": {},         # dict with two subdicts, either additive or subtractive with a list
    "debug": None,      # debug: bool
    "timeout": None,    # timeout: float
    "id_f": None,       # id file: path
    "f_date": None,     # from date: int
    "u_date": None,     # until date: int
    "res_tok": None,    # oai resumption token: str
    "conf_f": None,     # Configfile: path
    "conf_m": None,     # Configmode: bool
    "auto_m": None,     # Automode: bool
    "out_f": None,      # outputfile: path
    "n_procs": None,    # number of parallel downloads: int
    "mode": None,       # mode: str "ui" or "cli"
    "exp_type": None    # export type either "xml" or "json"
}


def print_and_log(message, logger, type: str, end="\n"):
    print(message, end)
    for placeholder in [SEP_LINE, ACHTUNG, INFO, FEHLER]:
        if placeholder in message:
            message = message.replace(placeholder, '')
    if type == "info":
        logger.info(message)
    elif type == "warning":
        logger.warning(message)


def handle_error(e, mode, url=None):
    error_messages = {
        Timeout: "Timeout determining list size. The API is not reachable.",
        RetryError: "Identifier harvesting aborted due to too many retries. Is the API reachable?",
        HTTPError: f"Identifier harvesting aborted due to an HTTP error."
    }
    if type(e) is ConnectionError:
        if "404" in str(e):
            print_and_exit("The API is not reachable. Is the URL correct?", mode)
        elif errors := re.findall(r"error\scode=['\"](.+)['\"]>(.*)<\\error", str(e)):
            print_and_exit(f"{FEHLER} API error: {errors[0][0]}/{errors[0][1]} at {url}", mode)
    elif type(e) in error_messages:
        print_and_exit(error_messages[type(e)], mode, e)
    else:
        print_and_exit("An unexpected error occurred.", mode, e)


def print_and_exit(message, mode=None, exception=None):
    logger.critical(message)
    if exception:
        logger.exception("Exception details:", exc_info=exception)
    if mode == "ui" and input(f"{message}\nDrücken Sie Enter zum Beenden..."):
        sys.exit()


def isinvalid_xml_content(response, url, mode):
    try:
        root = etree.XML(response.content)
    except etree.XMLSyntaxError as e:
        print_and_exit(f"XML Syntax Error in the API response, probably no valid XML ({url}): '{e}'", mode)
    return root


# restliche Funktionen
def get_identifier(PRM: dict, url: str, session) -> list:
    """

    :param PRM: dict of a
    :param url:
    :param session:
    :return: a list of all extracted identifiers
    """
    spinner = Halo(text="Lade IDs...", spinner="dotes3")
    spinner.start()
    id_list = []
    while True:
        try:
            response = session.get(url, verify=False, timeout=(20, 80))
            root = isinvalid_xml_content(response, url, PRM["mode"])
        except (Timeout, RetryError, HTTPError, ConnectionError) as e:
            handle_error(e, PRM["mode"], url)

        # zu Beginn ListSize ermitteln
        if id_list == []:
            list_size = re.search(r"completeListSize=[\"|'](\d+)[\"|']", response.text)
            print_and_log(f"{INFO}Angegebene ListSize: {list_size.group(1)}", logger, "info", end="") if list_size else print("Keine ListSize angegeben")

        # Token auslesen
        token = root.findtext(f".//{NAMESPACE}resumptionToken")
        logger.info(f"Token: {token}") if PRM["debug"] else None

        # Die Objekte in listIdentifiers auslesen
        generated_ids = [ids.text for ids in root.findall(f".//{NAMESPACE}identifier")]
        id_list.extend(generated_ids)

        # URL für nächste Suche zusammenbauen, wenn kein Token (== letzte Seite) loop beenden
        if not token:
            break
        url = f"{PRM['b_url']}?verb=ListIdentifiers&resumptionToken={token}"

    spinner.succeed(f"Identifier Harvesting beendet. Insgesamt {len(id_list)} IDs bekommen.")
    logger.info(f"Letzte abgefragte URL: {PRM['b_url']}\nIdentifier Harvesting beendet.")

    return id_list


def harvest_files(ids, PRM, folder, session) -> list:
    """
    Liest ID Liste und lädt die IDs einzeln über GetRecord.
    :param ids: 
    :param mode: 
    :return: failed_download, failed_ids ID-Lists
    """

    def save_file(oai_id: str, folder: str, response, export_type):
        filename = re.sub(r"([:.|&%$=()\"#+\'´`*~<>!?/;,\[\]]|\s)", "_", oai_id)
        if export_type == "xml":
            with open(os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8") as of:
                of.write(response)
        elif export_type == "json":
            try:
                xml_data = xmltodict.parse(response)
                with open(os.path.join(folder, f"{filename}.json"), "w", encoding="utf8") as of:
                    json.dump(xml_data, of, indent=2)
            except Exception as e:
                print(f"Error converting XML to JSON: {e}")
        else:
            print(f"Unsupported file type: {export_type}")

    def get_text(url, session, folder, export_type):
        oai_id = parse_qs(urlparse(url).query)["identifier"][0]
        try:
            with session.get(url) as resp:
                if resp.status_code != 200:
                    logger.critical(f"Statuscode {resp.status_code} bei {url}")
                    return {"failed_download": oai_id}
                if errors := re.findall(r'error\scode="(.+)">(.+)<\\error', resp.text):
                    logger.warning(f"Datei {url} konnte nicht geharvestet werden ('{errors[0][0]}')")
                    return {"failed_download": oai_id}
                save_file(oai_id, folder, resp.text, export_type)
        except RequestException as e:
            logger.warning(f"Datei {url} wurde aufgrund eines {type(e).__name__} übersprungen: {e}")
            return {"failed_id": oai_id}

    if ids:
        print_and_log(f"{SEP_LINE}Harveste {len(ids)} IDs.", logger, "info")
    else:
        print_and_exit(f"{SEP_LINE}Keine Identifier bekommen, breche ab.", PRM["mode"])

    urls = [PRM["b_url"] + "?verb=GetRecord&metadataPrefix=" + PRM["pref"] + "&identifier=" + str(id) for id in ids]

    with ThreadPool(PRM["n_procs"]) as pool:
        failed_oaiids = list(
            tqdm(
                pool.imap(partial(get_text, session=session, folder=folder, export_type=PRM["exp_type"]), urls),
                total=len(urls),
                desc=r"Harveste...",
                unit="files",
                dynamic_ncols=True,
            )
        )

    failed_files = defaultdict(list)
    for subdict in failed_oaiids:
        if subdict and isinstance(subdict, dict):
            for key, value in subdict.items():
                failed_files[key].append(value)
    failed_ids, failed_download = failed_files.get("failed_id", []), failed_files.get("failed_download", [])
    logger.info("Harvesting beendet")

    return failed_download, failed_ids


def change_date(date: str, name: str, key: str):
    if PRM["conf_m"] and PRM["auto_m"]:
        print_and_log(f"{SEP_LINE}{INFO} Setze das {key} der Konfigurationsdatei {PRM['conf_f']} auf das aktuelle Datum", logger, "info")
        doc = yaml.safe_load(open(name))
        doc[key] = date
        with open(name, "w") as f:
            yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)


def create_id_file(p, ids, folder, type=None): # type kann außerdem failed sein
    file = os.path.join(folder, f"{type}_ids.yaml")
    with open(file, "w", encoding="utf-8") as f:
        f.write(f"Information: Liste erzeugt mit Ometha {__version__}\ndate: {TIMESTR}\nurl: {p['b_url']}\nset: {p['sets']}\nmprefix: {p['pref']}\nids:\n")
        f.write("\n".join([f"- '{fid}'" for fid in ids]))
    return file


def read_yaml_file(file_path, keys, default=None):
    try:
        with open(file_path, "r", encoding="utf-8") as ymlfile:
            return [yaml.safe_load(ymlfile).get([key], default) for key in keys]
    except (OSError, KeyError) as e:
        print_and_exit(f"{SEP_LINE}{FEHLER} Datei kann nicht gelesen werden.") if isinstance(e, OSError) else None
        print_and_exit(f"{SEP_LINE}Der Eintrag für {e} fehlt in der YAML Datei {file_path}.") if isinstance(e, KeyError) else None
