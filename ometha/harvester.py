import json
import os
import re
import sys
import time
import threading
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
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
                        token_file = os.path.join(PRM.get("out_f", "."), f"resumption_token_{TIMESTR}.txt")
                        try:
                            with open(token_file, "w") as f:
                                f.write(f"Resume with: --resumptiontoken {last_token}\n")
                                f.write(f"Last successful count: {len(id_list)} IDs\n")
                                f.write(f"Token: {last_token}\n")
                            print(f"\n{INFO}Resumption token saved to: {token_file}")
                            logger.info(f"Saved resumption token to {token_file}")
                        except Exception as save_err:
                            logger.error(f"Could not save resumption token: {save_err}")
                    spinner.stop()
                    handle_error(e, PRM["mode"], url)
                else:
                    wait_time = 10 * (2**retry_count)  # Exponential backoff: 20s, 40s, 80s
                    print(f"\n{ACHTUNG}Retry {retry_count}/{max_retries} after {wait_time}s due to error...")
                    logger.warning(f"Retrying after {wait_time}s (attempt {retry_count}/{max_retries})")
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
            token_file = os.path.join(PRM.get("out_f", "."), "resumption_token_latest.txt")
            try:
                with open(token_file, "w") as f:
                    f.write(f"Resume with: --resumptiontoken {token}\n")
                    f.write(f"Current count: {len(id_list)} IDs\n")
                    f.write(f"Token: {token}\n")
                logger.info(f"Saved checkpoint at {len(id_list)} IDs")
            except Exception as save_err:
                logger.warning(f"Could not save resumption token checkpoint: {save_err}")

        # URL für nächste Suche zusammenbauen, wenn kein Token (== letzte Seite) loop beenden
        if not token:
            break
        url = f"{PRM['b_url']}?verb=ListIdentifiers&resumptionToken={token}"

    spinner.text = f"Identifier Harvesting beendet. Insgesamt {len(id_list)} IDs bekommen."
    spinner.ok("✓")
    logger.info(f"Letzte abgefragte URL: {url}")
    logger.info("Identifier Harvesting beendet.")

    return id_list


def harvest_files(ids: list[str], PRM: dict[str, Any], folder: str, session: Session) -> tuple[list[str], list[str]]:
    """
    Liest ID Liste und lädt die IDs einzeln über GetRecord.
    :param ids: List of IDs
    :param PRM: dict of parameters
    :param folder: str of the folder path
    :return: tuple with failed_download, failed_ids ID-Lists
    """

    def get_subfolder(base_folder: str, index: int, batch_size: int = 10_000) -> str:
        """Gibt den Unterordner für eine Datei anhand ihres Index zurück.

        Teilt die Downloads in Unterordner à batch_size Dateien auf,
        z.B. part_0001, part_0002 usw.

        Args:
            base_folder: Basisordner für alle Downloads.
            index: Laufender Index der aktuellen Datei (0-basiert).
            batch_size: Maximale Anzahl von Dateien pro Unterordner.

        Returns:
            Pfad des Unterordners (wird bei Bedarf angelegt).
        """
        batch_num = index // batch_size + 1
        subfolder = os.path.join(base_folder, f"part_{batch_num:04d}")
        os.makedirs(subfolder, exist_ok=True)
        return subfolder

    def save_file(oai_id: str, folder: str, response: str, export_type: str) -> bool:
        """Save an OAI record response to a file in the given folder.

        The filename is derived from the OAI identifier with special characters replaced.
        Saves as XML or JSON depending on export_type; falls back to XML for unknown types.

        Returns:
            True on success, False if the file could not be saved (e.g. JSON conversion error).
        """
        filename = re.sub(r"([:.|&%$=()\"#+\'´`*~<>!?/;,\[\]]|\s)", "_", oai_id)
        if export_type == "xml":
            with open(os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8") as of:
                of.write(response)
            return True
        elif export_type == "json":
            try:
                xml_data = xmltodict.parse(response)
                with open(os.path.join(folder, f"{filename}.json"), "w", encoding="utf8") as of:
                    json.dump(xml_data, of, indent=2)
                return True
            except Exception as e:
                logger.warning(f"XML→JSON-Konvertierung fehlgeschlagen für {oai_id}: {e}")
                return False
        else:
            # TODO this check should be done before the harvesting starts
            with open(os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8") as of:
                of.write(response)
            return True

    def get_response_text_from_url(url_with_index: tuple[str, int], session: Session, folder: str, export_type: str) -> dict[str, str] | None:
        """
        Downloads the content from the given URL and saves it to a file.

        Parameters:
        url_with_index (tuple[str, int]): The URL and its index in the overall download list.
        session (requests.Session): The session to use for the download.
        folder (str): The base folder; files are saved into numbered subfolders of 10,000 each.
        export_type (str): The type of the export (used to determine if saved as XML or JSON).

        Returns (only in case of an error):
        dict: A dictionary containing the id of the failed download or failed id in case of an error.
        """
        url, index = url_with_index
        target_folder = get_subfolder(folder, index)
        oai_id = parse_qs(urlparse(url).query)["identifier"][0]
        try:
            with session.get(url) as resp:
                if resp.status_code != 200:
                    logger.critical(f"Statuscode {resp.status_code} bei {url}")
                    return {"failed_download": oai_id}
                if errors := re.findall(r'error\scode="(.+)">(.+)</error>', resp.text):
                    logger.warning(f"Datei {url} konnte nicht geharvestet werden ('{errors[0][0]}')")
                    return {"failed_download": oai_id}
                if not save_file(oai_id, target_folder, resp.text, export_type):
                    return {"failed_download": oai_id}
        except RequestException as e:
            logger.warning(f"Datei {url} wurde aufgrund eines {type(e).__name__} übersprungen: {e}")
            return {"failed_id": oai_id}

    if ids:
        print_and_log(f"{SEP_LINE}Harveste {len(ids)} IDs.", logger, "info")
    else:
        log_critical_and_print_and_exit(f"{SEP_LINE}Keine Identifier bekommen, breche ab.", PRM["mode"])

    urls = [
        (PRM["b_url"] + "?verb=GetRecord&metadataPrefix=" + PRM["pref"] + "&identifier=" + str(id), index)
        for index, id in enumerate(ids)
    ]

    # ------------------------------------------------------------------ #
    # Adaptiver Worker-Pool                                                #
    #                                                                      #
    # Ein Steuer-Thread misst alle MEASURE_INTERVAL Sekunden den          #
    # Durchsatz (Dateien/s) über ein gleitendes Fenster der letzten       #
    # WINDOW_SIZE Messungen und passt die Worker-Anzahl an:               #
    #   - Durchsatz gestiegen  → Worker erhöhen (max. 100)                #
    #   - Durchsatz gesunken   → Worker reduzieren (min. 1)               #
    #   - Durchsatz stabil     → keine Änderung                           #
    #                                                                      #
    # Da ThreadPoolExecutor keine direkte Größenänderung unterstützt,     #
    # wird die gewünschte Parallelität über ein threading.Semaphore        #
    # gesteuert: jeder Worker-Thread muss vor dem Request ein Slot         #
    # belegen und gibt ihn danach frei.                                    #
    # ------------------------------------------------------------------ #

    MEASURE_INTERVAL = 5      # Sekunden zwischen zwei Messungen
    WINDOW_SIZE      = 4      # Anzahl der Messungen im gleitenden Fenster
    STEP_UP          = 2      # Worker-Erhöhung bei verbessertem Durchsatz
    STEP_DOWN        = 1      # Worker-Reduzierung bei verschlechtertem Durchsatz
    THROUGHPUT_TOL   = 0.05   # ±5 % gelten als "stabil" (keine Änderung)
    MAX_WORKERS      = min(PRM["n_procs"], 100)
    MIN_WORKERS      = 1

    n_workers        = MAX_WORKERS
    semaphore        = threading.Semaphore(n_workers)
    lock             = threading.Lock()      # schützt n_workers + semaphore
    stop_event       = threading.Event()
    completed        = threading.local()     # thread-lokaler Zähler (nicht genutzt)
    done_count       = [0]                   # [0] → mutierbare Referenz aus Closure
    throughput_window: deque[float] = deque(maxlen=WINDOW_SIZE)
    worker_snapshots: list[int] = [n_workers]  # Worker-Anzahl nach jeder Messung

    def adaptive_controller() -> None:
        """Beobachtet den Durchsatz und passt n_workers + Semaphore laufend an."""
        nonlocal n_workers
        last_done = 0
        last_time = time.monotonic()

        while not stop_event.is_set():
            stop_event.wait(timeout=MEASURE_INTERVAL)
            if stop_event.is_set():
                break

            now      = time.monotonic()
            elapsed  = now - last_time or 1e-9
            current  = done_count[0]
            delta    = current - last_done

            throughput = delta / elapsed          # Dateien / Sekunde
            throughput_window.append(throughput)
            last_done = current
            last_time = now

            if len(throughput_window) < 2:
                continue                          # noch zu wenig Daten

            avg_prev = sum(list(throughput_window)[:-1]) / (len(throughput_window) - 1)
            avg_now  = throughput_window[-1]

            if avg_prev == 0:
                continue

            change = (avg_now - avg_prev) / avg_prev   # relative Änderung

            with lock:
                old = n_workers
                if change > THROUGHPUT_TOL and n_workers < MAX_WORKERS:
                    # Durchsatz besser → mehr Worker
                    delta_w = min(STEP_UP, MAX_WORKERS - n_workers)
                    n_workers += delta_w
                    for _ in range(delta_w):
                        semaphore.release()
                    logger.info(
                        f"[AdaptivePool] Durchsatz ↑ {avg_now:.2f} f/s "
                        f"(+{change*100:.1f}%) → Worker {old} → {n_workers}"
                    )
                elif change < -THROUGHPUT_TOL and n_workers > MIN_WORKERS:
                    # Durchsatz schlechter → weniger Worker
                    delta_w = min(STEP_DOWN, n_workers - MIN_WORKERS)
                    n_workers -= delta_w
                    # Slots entziehen: beim nächsten acquire() greift die neue Zahl
                    for _ in range(delta_w):
                        semaphore.acquire(blocking=False)
                    logger.info(
                        f"[AdaptivePool] Durchsatz ↓ {avg_now:.2f} f/s "
                        f"({change*100:.1f}%) → Worker {old} → {n_workers}"
                    )
                else:
                    logger.debug(
                        f"[AdaptivePool] Durchsatz stabil {avg_now:.2f} f/s "
                        f"({change*100:+.1f}%) → Worker bleibt {n_workers}"
                    )
                worker_snapshots.append(n_workers)

    def throttled_download(url_with_index: tuple[str, int]) -> dict[str, str] | None:
        """Wrapper: belegt einen Semaphore-Slot, führt den Download durch, gibt ihn frei."""
        semaphore.acquire()
        try:
            result = get_response_text_from_url(
                url_with_index,
                session=session,
                folder=folder,
                export_type=PRM["exp_type"],
            )
            with lock:
                done_count[0] += 1
            return result
        finally:
            semaphore.release()

    controller_thread = threading.Thread(target=adaptive_controller, daemon=True)
    controller_thread.start()
    logger.info(f"[AdaptivePool] Start mit {n_workers} Workern (max. {MAX_WORKERS})")

    failed_oaiids: list[dict[str, str] | None] = []
    # MAX_WORKERS Threads im Pool – die effektive Parallelität regelt das Semaphore
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(throttled_download, u): u for u in urls}
        with tqdm(total=len(urls), desc="Harveste...", unit="files", dynamic_ncols=True) as pbar:
            for future in as_completed(futures):
                failed_oaiids.append(future.result())
                pbar.update(1)
                with lock:
                    pbar.set_postfix(workers=n_workers)

    stop_event.set()
    controller_thread.join(timeout=MEASURE_INTERVAL + 1)

    w_min  = min(worker_snapshots)
    w_max  = max(worker_snapshots)
    w_avg  = sum(worker_snapshots) / len(worker_snapshots)
    stats_msg = f"[AdaptivePool] Worker-Statistik: min. {w_min}, max. {w_max}, Ø {w_avg:.1f} Threads"
    print_and_log(stats_msg, logger, "info")

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
        doc = {k: v for k, v in doc.items() if v is not None}
        with open(name, "w") as f:
            yaml.safe_dump(doc, f, default_flow_style=False, sort_keys=False)


def create_id_file(PRM: dict[str, Any], ids: list[str], folder: str, type: str | None = None) -> str:
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
        log_critical_and_print_and_exit(f"\n{FEHLER} Datei {file_path} kann nicht gelesen werden.")
        sys.exit()
    except KeyError as e:
        log_critical_and_print_and_exit(f"Der Eintrag für {e} fehlt in der YAML Datei {file_path}.")