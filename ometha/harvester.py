import json
import os
import re
import sys
import time
from collections import defaultdict
from collections.abc import Callable
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from typing import Any
from urllib.parse import parse_qs, urlparse

import xmltodict
import yaml
from yaspin import yaspin
from yaspin.spinners import Spinners
from loguru import logger
from requests import Session
from requests.exceptions import (
    RequestException,
)
from tqdm import tqdm

from ._version import __version__
from .helpers import (
    ACHTUNG,
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
def get_identifier(
    PRM: dict[str, Any],
    url: str,
    session: Session,
    on_list_size: Callable[[int], None] | None = None,
) -> list[str]:
    """Harvest all identifiers from a ListIdentifiers OAI-PMH endpoint.

    Iterates through all pages using resumption tokens, saves checkpoint tokens
    periodically, and retries on transient failures with exponential backoff.

    Args:
        PRM: Parameters dictionary with harvesting configuration.
        url: Initial ListIdentifiers URL.
        session: HTTP session to use for requests.
        on_list_size: Optional callback invoked with the total list size once known.

    Returns:
        A list of all harvested OAI identifier strings.
    """
    spinner = yaspin(Spinners.dots)
    spinner.start()

    id_list = []
    token_save_interval = 1000  # Save resumption token every 1000 IDs
    last_token = None

    while True:
        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            try:
                response = session.get(url, verify=False, timeout=(30, 120))
                root = isinvalid_xml_content(response, url, PRM["mode"])
                break  # Success, exit retry loop
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    # Save the last resumption token before exiting
                    if last_token:
                        token_file = os.path.join(
                            PRM.get("out_f", "."), f"resumption_token_{TIMESTR}.txt"
                        )
                        try:
                            with open(token_file, "w") as f:
                                f.write(
                                    f"Resume with: --resumptiontoken {last_token}\n"
                                )
                                f.write(f"Last successful count: {len(id_list)} IDs\n")
                                f.write(f"Token: {last_token}\n")
                            print(f"\n{INFO}Resumption token saved to: {token_file}")
                            logger.info(f"Saved resumption token to {token_file}")
                        except Exception as save_err:
                            logger.error(f"Could not save resumption token: {save_err}")
                    spinner.stop()
                    handle_error(e, PRM["mode"], url)
                else:
                    wait_time = 10 * (
                        2**retry_count
                    )  # Exponential backoff: 20s, 40s, 80s
                    print(
                        f"\n{ACHTUNG}Retry {retry_count}/{max_retries} after {wait_time}s due to error..."
                    )
                    logger.warning(
                        f"Retrying after {wait_time}s (attempt {retry_count}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue

        # zu Beginn ListSize ermitteln
        if id_list == []:
            list_size = re.search(r"completeListSize=[\"|'](\d+)[\"|']", response.text)
            print_and_log(
                f"{INFO}Angegebene ListSize: {list_size.group(1)}",
                logger,
                "info",
                end="",
            ) if list_size else print("Keine ListSize angegeben")
            # Callback Funktion für die GUI, damit sie die ListSize anzeigen kann, wenn sie vorhanden ist
            if list_size and on_list_size:
                on_list_size(int(list_size.group(1)))

        # Token auslesen
        token = root.findtext(f".//{NAMESPACE}resumptionToken")
        if token:
            last_token = token
        logger.info(f"Token: {token}") if PRM["debug"] else None

        # Die Objekte in listIdentifiers auslesen
        generated_ids = [ids.text for ids in root.findall(f".//{NAMESPACE}identifier")]
        id_list.extend(generated_ids)
        spinner.text = "Lade IDs: " + str(len(id_list))

        # Periodically save resumption token for recovery
        if token and len(id_list) % token_save_interval == 0:
            token_file = os.path.join(
                PRM.get("out_f", "."), "resumption_token_latest.txt"
            )
            try:
                with open(token_file, "w") as f:
                    f.write(f"Resume with: --resumptiontoken {token}\n")
                    f.write(f"Current count: {len(id_list)} IDs\n")
                    f.write(f"Token: {token}\n")
                logger.info(f"Saved checkpoint at {len(id_list)} IDs")
            except Exception as save_err:
                logger.warning(
                    f"Could not save resumption token checkpoint: {save_err}"
                )

        # URL für nächste Suche zusammenbauen, wenn kein Token (== letzte Seite) loop beenden
        if not token:
            break
        url = f"{PRM['b_url']}?verb=ListIdentifiers&resumptionToken={token}"

    spinner.text = (
        f"Identifier Harvesting beendet. Insgesamt {len(id_list)} IDs bekommen."
    )
    spinner.ok("✓")
    logger.info(f"Letzte abgefragte URL: {PRM['b_url']}")
    logger.info("Identifier Harvesting beendet.")

    return id_list


def harvest_files(
    ids: list[str], PRM: dict[str, Any], folder: str, session: Session
) -> tuple[list[str], list[str]]:
    """
    Liest ID Liste und lädt die IDs einzeln über GetRecord.
    :param ids: List of IDs
    :param PRM: dict of parameters
    :param folder: str of the folder path
    :return: tuple with failed_download, failed_ids ID-Lists
    """

    def save_file(oai_id: str, folder: str, response: str, export_type: str) -> bool:
        """Save an OAI record response to a file in the given folder.

        The filename is derived from the OAI identifier with special characters replaced.
        Saves as XML or JSON depending on export_type; falls back to XML for unknown types.

        Returns:
            True on success, False if the file could not be saved (e.g. JSON conversion error).
        """
        filename = re.sub(r"([:.|&%$=()\"#+\'´`*~<>!?/;,\[\]]|\s)", "_", oai_id)
        if export_type == "xml":
            with open(
                os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8"
            ) as of:
                of.write(response)
            return True
        elif export_type == "json":
            try:
                xml_data = xmltodict.parse(response)
                with open(
                    os.path.join(folder, f"{filename}.json"), "w", encoding="utf8"
                ) as of:
                    json.dump(xml_data, of, indent=2)
                return True
            except Exception as e:
                logger.warning(f"XML→JSON-Konvertierung fehlgeschlagen für {oai_id}: {e}")
                return False
        else:
            # TODO this check should be done before the harvesting starts
            with open(
                os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8"
            ) as of:
                of.write(response)
            return True

    def get_response_text_from_url(
        url: str, session: Session, folder: str, export_type: str
    ) -> dict[str, str] | None:
        """
        Downloads the content from the given URL and saves it to a file.

        Parameters:
        url (str): The URL to download the content from.
        session (requests.Session): The session to use for the download.
        folder (str): The folder to save the downloaded content in.
        export_type (str): The type of the export (used to determine if saved as XML or JSON).

        Returns (only in case of an error):
        dict: A dictionary containing the id of the failed download or failed id in case of an error.
        """
        oai_id = parse_qs(urlparse(url).query)["identifier"][0]
        try:
            with session.get(url) as resp:
                if resp.status_code != 200:
                    logger.critical(f"Statuscode {resp.status_code} bei {url}")
                    return {"failed_download": oai_id}
                if errors := re.findall(r'error\scode="(.+)">(.+)</error>', resp.text):
                    logger.warning(
                        f"Datei {url} konnte nicht geharvestet werden ('{errors[0][0]}')"
                    )
                    return {"failed_download": oai_id}
                if not save_file(oai_id, folder, resp.text, export_type):
                    return {"failed_download": oai_id}
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
                        get_response_text_from_url,
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


def change_date(date: str, name: str, key: str) -> None:
    """
    Utility function: Change the date in the configuration file.
    No return value, the function changes the file in place.
    """
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


def create_id_file(
    PRM: dict[str, Any], ids: list[str], folder: str, type: str | None = None
) -> str:
    """
    Create an ID file with the given parameters.

    Args:
        PRM (dict): The parameters dictionary.
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
            f"Information: Liste erzeugt mit Ometha {__version__}\ndate: {TIMESTR}\nbaseurl: {PRM['b_url']}\nsets: {PRM['sets']}\nmetadataPrefix: {PRM['pref']}\ndatengeber: {PRM['dat_geb']}\ntimeout: {PRM['timeout']}\ndebug: {PRM['debug']}\nfromdate: {PRM['f_date']}\nuntildate: {PRM['u_date']}\noutputfolder: {PRM['out_f']}\nids:\n"
        )
        f.write("\n".join([f"- '{fid}'" for fid in ids]))
    return file


def read_yaml_file(file_path: str, keys: list[str], default: Any = None) -> list[Any]:
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
    except OSError:
        log_critical_and_print_and_exit(
            f"\n{FEHLER} Datei {file_path} kann nicht gelesen werden."
        )
        sys.exit()
    except KeyError as e:
        log_critical_and_print_and_exit(
            f"Der Eintrag für {e} fehlt in der YAML Datei {file_path}."
        )
