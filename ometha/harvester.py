import json
import os
import re
import sys
from collections import defaultdict
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from urllib.parse import parse_qs, urlparse

import xmltodict
import yaml
from halo import Halo
from loguru import logger
from requests.exceptions import (
    ConnectionError,
    HTTPError,
    RequestException,
    RetryError,
    Timeout,
)
from tqdm import tqdm

from ._version import __version__
from .helpers import (
    FEHLER,
    INFO,
    NAMESPACE,
    PRM,
    SEP_LINE,
    TIMESTR,
    handle_error,
    isinvalid_xml_content,
    log_critical_and_print_and_exit,
    print_and_log,
)


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
            print_and_log(
                f"\n{INFO}Angegebene ListSize: {list_size.group(1)}",
                logger,
                "info",
                end="",
            ) if list_size else print("Keine ListSize angegeben")

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

    spinner.succeed(
        f"Identifier Harvesting beendet. Insgesamt {len(id_list)} IDs bekommen."
    )
    logger.info(f"Letzte abgefragte URL: {PRM['b_url']}")
    logger.info("Identifier Harvesting beendet.")

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
            with open(
                os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8"
            ) as of:
                of.write(response)
        elif export_type == "json":
            try:
                xml_data = xmltodict.parse(response)
                with open(
                    os.path.join(folder, f"{filename}.json"), "w", encoding="utf8"
                ) as of:
                    json.dump(xml_data, of, indent=2)
            except Exception as e:
                print(f"Error converting XML to JSON: {e}")
        else:
            # print(f"Unsupported file type: {export_type}. Reverting to XML.")
            # TODO this check should be done before the harvesting starts
            with open(
                os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8"
            ) as of:
                of.write(response)

    def get_text(url, session, folder, export_type):
        oai_id = parse_qs(urlparse(url).query)["identifier"][0]
        try:
            with session.get(url) as resp:
                if resp.status_code != 200:
                    logger.critical(f"Statuscode {resp.status_code} bei {url}")
                    return {"failed_download": oai_id}
                if errors := re.findall(r'error\scode="(.+)">(.+)<\\error', resp.text):
                    logger.warning(
                        f"Datei {url} konnte nicht geharvestet werden ('{errors[0][0]}')"
                    )
                    return {"failed_download": oai_id}
                save_file(oai_id, folder, resp.text, export_type)
        except RequestException as e:
            logger.warning(
                f"Datei {url} wurde aufgrund eines {type(e).__name__} übersprungen: {e}"
            )
            return {"failed_id": oai_id}

    if ids:
        print_and_log(f"{SEP_LINE}Harveste {len(ids)} IDs.", logger, "info")
    else:
        log_critical_and_print_and_exit(
            f"{SEP_LINE}Keine Identifier bekommen, breche ab.", PRM["mode"]
        )

    urls = [
        PRM["b_url"]
        + "?verb=GetRecord&metadataPrefix="
        + PRM["pref"]
        + "&identifier="
        + str(id)
        for id in ids
    ]

    with ThreadPool(PRM["n_procs"]) as pool:
        failed_oaiids = list(
            tqdm(
                pool.imap(
                    partial(
                        get_text,
                        session=session,
                        folder=folder,
                        export_type=PRM["exp_type"],
                    ),
                    urls,
                ),
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
    failed_ids, failed_download = (
        failed_files.get("failed_id", []),
        failed_files.get("failed_download", []),
    )
    logger.info("Harvesting beendet")

    return failed_download, failed_ids


def change_date(date: str, name: str, key: str):
    if PRM["conf_m"] and PRM["auto_m"]:
        print_and_log(
            f"{SEP_LINE}{INFO} Setze das {key} der Konfigurationsdatei {PRM['conf_f']} auf das aktuelle Datum",
            logger,
            "info",
        )
        doc = yaml.safe_load(open(name))
        doc[key] = date
        with open(name, "w") as f:
            yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)


def create_id_file(p, ids, folder, type=None):
    """
    Create an ID file with the given parameters.

    Args:
        p (dict): The parameters dictionary.
        ids (list): The list of IDs.
        folder (str): The folder path where the file will be created.
        type (str, optional): The type of the file. Defaults to None.

    Returns:
        str: The path of the created file.
    """
    # TODO add date or some other kind of identifier to the file name?
    file = os.path.join(folder, f"_ometha_{type}_ids.yaml")
    with open(file, "w", encoding="utf-8") as f:
        f.write(
            f"Information: Liste erzeugt mit Ometha {__version__}\ndate: {TIMESTR}\nbaseurl: {p['b_url']}\nset: {p['sets']}\nmetadataPrefix: {p['pref']}\ndatengeber: {p['dat_geb']}\ntimeout: {p['timeout']}\ndebug: {p['debug']}\nfromdate: {p['f_date']}\nuntildate: {p['u_date']}\noutputfolder: {p['out_f']}\nids:\n"
        )
        f.write("\n".join([f"- '{fid}'" for fid in ids]))
    return file


def read_yaml_file(file_path: str, keys: list, default: any = None) -> list:
    """Reads a yaml file and returns the values for the given keys.

    Args:
        file_path: The path to the yaml file.
        keys: The keys to read from the file.
        default: The default value to return if the key is not found. Defaults to None.

    Raises:
        FileNotFoundError: If the file does not exist.
        KeyError: If a key is not found in the file.

    Returns:
        A list containing the values for the given keys.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as ymlfile:
            y = yaml.safe_load(ymlfile)
            result = []
            for key in keys:
                value = y.get(key, default)
                result.append(value)
            return result
    except OSError as e:
        log_critical_and_print_and_exit(
            f"\n{FEHLER} Datei {file_path} kann nicht gelesen werden."
        )
        sys.exit()
    except KeyError as e:
        log_critical_and_print_and_exit(
            f"Der Eintrag für {e} fehlt in der YAML Datei {file_path}."
        )
