import json
import queue
import os
import re
import sys
import time
import threading
from collections import defaultdict, deque
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import parse_qs, urlparse

import xmltodict
import yaml
from lxml import etree
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
    OAITIMESTR,
    PRM,
    SEP_LINE,
    TIMESTR,
    handle_error,
    isinvalid_xml_content,
    log_critical_and_print_and_exit,
    print_and_log,
    safe_get,
)


def _build_yaml_doc(PRM: dict, extra: dict | None = None) -> dict:
    """Build a sanitized dict suitable for yaml.safe_dump().

    Normalizes sets to a flat list, ensures debug defaults to False,
    and keeps all other values as-is (yaml.safe_dump handles None → null).

    Args:
        PRM: Parameters dictionary.
        extra: Additional key/value pairs merged into the document
               (e.g. ``{"resumptiontoken": token}`` for checkpoints).

    Returns:
        A dict ready for ``yaml.safe_dump()``.
    """
    # Normalize sets to a flat list
    sets_val = PRM.get("sets")
    if isinstance(sets_val, list) and sets_val and isinstance(sets_val[0], dict):
        additive = sets_val[0].get("additive") or []
        intersection = sets_val[0].get("intersection") or []
        sets_out = additive + intersection
    elif isinstance(sets_val, list):
        sets_out = sets_val or []
    else:
        sets_out = []

    doc = {
        "baseurl":           PRM.get("b_url"),
        "metadataPrefix":    PRM.get("pref"),
        "datengeber":        PRM.get("dat_geb"),
        "sets":              sets_out,
        "fromdate":          PRM.get("f_date"),
        "untildate":         PRM.get("u_date"),
        "outputfolder":      PRM.get("out_f"),
        "numberofprocesses": PRM.get("n_procs"),
        "maxtimeout":        PRM.get("max_timeout"),
        "batchsize":         PRM.get("batch_size"),
        "timeout":           PRM.get("timeout"),
        "debug":             PRM.get("debug") or False,
    }
    if extra:
        doc.update(extra)
    return doc


def write_checkpoint_yaml(token: str, id_count: int, PRM: dict, label: str = "Laufender Checkpoint") -> str:
    """Schreibt einen Checkpoint mit Resumption Token als valide YAML-Datei.

    Verwendet yaml.safe_dump() statt f-Strings, damit None → null und komplexe
    Strukturen (sets, Listen) korrekt serialisiert werden.

    Args:
        token: Aktueller Resumption Token.
        id_count: Anzahl bisher gesammelter IDs.
        PRM: Parameter-Dictionary.
        label: Beschriftung im Information-Feld.

    Returns:
        Pfad der geschriebenen Datei.
    """
    is_abort = "Abbruch" in label
    filename = f"resumption_token_{TIMESTR}.yaml" if is_abort else f"resumption_token_latest_{TIMESTR}.yaml"
    token_file = os.path.join(PRM.get("out_f", "."), filename)

    doc = {
        "Information": f"{label} erzeugt mit Ometha {__version__}",
        "date": TIMESTR,
        **_build_yaml_doc(PRM, extra={"resumptiontoken": token}),
    }
    with open(token_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        f.write(f"# IDs bisher gesammelt: {id_count}\n")
        f.write(f"# Fortsetzen mit: ometha conf -c {token_file}\n")
    return token_file


def preview_list_size(url: str, session: Session) -> int | None:
    """Fragt die completeListSize für eine ListIdentifiers-URL ab, ohne zu paginieren.

    Für eine Vorschau vor dem eigentlichen Harvesting-Start (z. B. im GUI-Modus),
    damit Nutzer:innen sehen können, wie viele Records ihre aktuelle Auswahl
    (Sets, Zeitraum) ergibt, bevor sie den Lauf tatsächlich starten.

    Args:
        url: Vollständige ListIdentifiers-URL (inkl. metadataPrefix, ggf. set/from/until).
        session: HTTP session to use for the request.

    Returns:
        Die completeListSize als int, oder None wenn die Schnittstelle sie nicht
        angibt (laut OAI-PMH-Spec optional) oder der Request fehlschlägt.
    """
    try:
        response = safe_get(session, url, timeout=(20, 60))
        response.raise_for_status()
        etree.XML(response.content)  # nur Validität prüfen, root wird nicht gebraucht
    except Exception as e:
        logger.warning(f"ListSize-Vorschau fehlgeschlagen für {url}: {e}")
        return None
    list_size = re.search(r"completeListSize=[\"|\'](\d+)[\"|\']", response.text)
    return int(list_size.group(1)) if list_size else None


def get_identifier_pages(
    PRM: dict[str, Any],
    url: str,
    session: Session,
    on_list_size: Callable[[int], None] | None = None,
):
    """Generator: yields one page of OAI identifiers at a time (pipelining)."""
    token_save_interval = 1000
    last_token = None
    total_yielded = 0
    first_page = True

    while True:
        retry_count = 0
        max_retries = 3
        while retry_count < max_retries:
            try:
                response = safe_get(session, url, timeout=(30, 120))
                response.raise_for_status()
                root = isinvalid_xml_content(response, url, PRM["mode"])
                break
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    if last_token:
                        try:
                            token_file = write_checkpoint_yaml(last_token, total_yielded, PRM, label="Abbruch-Checkpoint")
                            print(f"\n{INFO}Checkpoint gespeichert: {token_file}")
                        except Exception as save_err:
                            logger.error(f"Could not save resumption token: {save_err}")
                    handle_error(e, PRM["mode"], url)
                else:
                    wait_time = 10 * (2**retry_count)
                    print(f"\n{ACHTUNG}Retry {retry_count}/{max_retries} after {wait_time}s...")
                    time.sleep(wait_time)
                    continue

        if first_page:
            first_page = False
            list_size = re.search(r"completeListSize=[\"|\'](\d+)[\"|\']", response.text)
            print_and_log(
                f"{INFO}Angegebene ListSize: {list_size.group(1)}",
                logger, "info", end="",
            ) if list_size else print("Keine ListSize angegeben")
            if list_size and on_list_size:
                on_list_size(int(list_size.group(1)))

        token = root.findtext(f".//{NAMESPACE}resumptionToken") or None
        if token and token.strip():
            token = token.strip()
            last_token = token
        else:
            token = None
        logger.info(f"Token: {token}") if PRM["debug"] else None

        page_ids = [ids.text for ids in root.findall(f".//{NAMESPACE}identifier")]
        total_yielded += len(page_ids)

        if token and total_yielded % token_save_interval == 0:
            try:
                write_checkpoint_yaml(token, total_yielded, PRM)
            except Exception as save_err:
                logger.warning(f"Could not save checkpoint: {save_err}")

        yield page_ids

        if not token:
            if last_token:
                try:
                    write_checkpoint_yaml(last_token, total_yielded, PRM, label="Abschluss-Checkpoint")
                except Exception:
                    pass
            break
        url = f"{PRM['b_url']}?verb=ListIdentifiers&resumptionToken={token}"


def get_identifier(
    PRM: dict[str, Any],
    url: str,
    session: Session,
    on_list_size: Callable[[int], None] | None = None,
) -> list[str]:
    """Wrapper around get_identifier_pages for backwards compatibility."""
    spinner = yaspin(Spinners.dots)
    spinner.start()
    id_list: list[str] = []
    try:
        for page_ids in get_identifier_pages(PRM, url, session, on_list_size):
            id_list.extend(page_ids)
            spinner.text = "Lade IDs: " + str(len(id_list))
    finally:
        spinner.text = f"Identifier Harvesting beendet. Insgesamt {len(id_list)} IDs bekommen."
        spinner.ok("✓")
        logger.info("Identifier Harvesting beendet.")
    return id_list


def harvest_files(
    ids: list[str],
    PRM: dict[str, Any],
    folder: str,
    session: Session,
    total_holder: list | None = None,
    stats: dict | None = None,
) -> tuple[list[str], list[str]]:
    """
    Liest ID Liste und lädt die IDs einzeln über GetRecord.
    :param ids: List of IDs, or an iterator/generator yielding pages of IDs (pipelining)
    :param PRM: dict of parameters
    :param folder: str of the folder path
    :param session: HTTP session to use for requests
    :param total_holder: optional single-element list; when ids is a generator, this
        gets filled in with the expected total ID count (from completeListSize) as
        soon as it becomes known, so the progress bar can display an ETA
    :param stats: optional dict, populated in place with run counts (successful,
        failed_download, failed_ids, skipped_deleted, moved_deleted, moved_invalid)
        for use in a run summary
    :return: tuple with failed_download, failed_ids ID-Lists
    """

    def get_subfolder(base_folder: str, index: int) -> str:
        """Gibt den Unterordner für eine Datei anhand ihres Index zurück.

        Teilt die Downloads in Unterordner à batch_size Dateien auf,
        z.B. part_0001, part_0002 usw. batch_size wird aus PRM["batch_size"]
        gelesen, default 100.000.
        """
        batch_size = int(PRM["batch_size"]) if PRM.get("batch_size") else 100_000
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
        filename = re.sub(r"([:.|&%$=()\"#+\'´`*~<>!?/;,$$]|\s)", "_", oai_id)
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
            with open(os.path.join(folder, f"{filename}.xml"), "w", encoding="utf8") as of:
                of.write(response)
            return True

    def is_deleted(response_text: str) -> bool:
        try:
            root = etree.fromstring(response_text.encode())
            header = root.find(f".//{NAMESPACE}header")
            return header is not None and header.get("status") == "deleted"
        except Exception:
            return False

    def is_invalid(response_text: str) -> bool:
        try:
            root = etree.fromstring(response_text.encode())
            return not root.findall(f".//{NAMESPACE}metadata/*/*")
        except Exception:
            return True

    _rate_limit_lock = threading.Lock()
    _last_request_time = [0.0]

    QUEUE_SIZE = 500
    id_queue: queue.Queue = queue.Queue(maxsize=QUEUE_SIZE)
    _SENTINEL = object()
    enqueued_count = [0]

    def producer() -> None:
        try:
            if isinstance(ids, list):
                if not ids:
                    log_critical_and_print_and_exit(
                        f"{SEP_LINE}Keine Identifier bekommen, breche ab.", PRM["mode"]
                    )
                print_and_log(f"{SEP_LINE}Harveste {len(ids)} IDs.", logger, "info")
                for oai_id in ids:
                    id_queue.put(oai_id)
                    enqueued_count[0] += 1
            else:
                for page_ids in ids:
                    for oai_id in page_ids:
                        id_queue.put(oai_id)
                        enqueued_count[0] += 1
        finally:
            id_queue.put(_SENTINEL)

    if isinstance(ids, list) and len(ids) == 0:
        log_critical_and_print_and_exit(f"{SEP_LINE}Keine Identifier bekommen, breche ab.", PRM["mode"])

    # ------------------------------------------------------------------ #
    # Gemeinsame Steuerparameter                                           #
    # ------------------------------------------------------------------ #
    MEASURE_INTERVAL = 5
    WINDOW_SIZE      = 4
    STEP_UP          = 2
    STEP_DOWN        = 1
    THROUGHPUT_TOL   = 0.05

    # Adaptiver Timeout
    TIMEOUT_STEP   = 5
    LATENCY_TOL    = 0.10
    MIN_TIMEOUT    = PRM["timeout"] if PRM["timeout"] else 0
    MAX_TIMEOUT    = int(PRM["max_timeout"]) if PRM.get("max_timeout") else 120

    MAX_WORKERS    = min(PRM["n_procs"], 100)
    MIN_WORKERS    = 1

    lock             = threading.Lock()
    stop_event       = threading.Event()

    n_workers        = MAX_WORKERS
    semaphore        = threading.Semaphore(n_workers)
    done_count       = [0]
    throughput_window: deque[float] = deque(maxlen=WINDOW_SIZE)
    worker_snapshots: list[int] = [n_workers]

    current_timeout  = [float(MIN_TIMEOUT)]
    latency_window:  deque[float] = deque(maxlen=WINDOW_SIZE * MAX_WORKERS)
    timeout_snapshots: list[float] = [current_timeout[0]]

    def adaptive_controller() -> None:
        """Beobachtet Durchsatz und Latenz; passt Worker-Anzahl und Timeout laufend an."""
        nonlocal n_workers
        last_done = 0
        last_time = time.monotonic()

        while not stop_event.is_set():
            stop_event.wait(timeout=MEASURE_INTERVAL)
            if stop_event.is_set():
                break

            now     = time.monotonic()
            elapsed = now - last_time or 1e-9
            current = done_count[0]
            delta   = current - last_done

            throughput = delta / elapsed
            throughput_window.append(throughput)
            last_done = current
            last_time = now

            with lock:
                latency_sample = list(latency_window)

            # Worker anpassen
            if len(throughput_window) >= 2:
                avg_prev = sum(list(throughput_window)[:-1]) / (len(throughput_window) - 1)
                avg_now  = throughput_window[-1]
                if avg_prev > 0:
                    change = (avg_now - avg_prev) / avg_prev
                    with lock:
                        old = n_workers
                        if change > THROUGHPUT_TOL and n_workers < MAX_WORKERS:
                            delta_w = min(STEP_UP, MAX_WORKERS - n_workers)
                            n_workers += delta_w
                            for _ in range(delta_w):
                                semaphore.release()
                            logger.info(f"[AdaptivePool] Durchsatz ↑ {avg_now:.2f} f/s (+{change*100:.1f}%) → Worker {old} → {n_workers}")
                        elif change < -THROUGHPUT_TOL and n_workers > MIN_WORKERS:
                            delta_w = min(STEP_DOWN, n_workers - MIN_WORKERS)
                            n_workers -= delta_w
                            for _ in range(delta_w):
                                semaphore.acquire(blocking=False)
                            logger.info(f"[AdaptivePool] Durchsatz ↓ {avg_now:.2f} f/s ({change*100:.1f}%) → Worker {old} → {n_workers}")
                        else:
                            logger.debug(f"[AdaptivePool] Durchsatz stabil {avg_now:.2f} f/s ({change*100:+.1f}%) → Worker bleibt {n_workers}")
                        worker_snapshots.append(n_workers)

            # Timeout anpassen
            if MAX_TIMEOUT > 0 and len(latency_sample) >= 2:
                sorted_lat = sorted(latency_sample)
                mid = len(sorted_lat) // 2
                median_lat = (sorted_lat[mid - 1] + sorted_lat[mid]) / 2 if len(sorted_lat) % 2 == 0 else sorted_lat[mid]
                with lock:
                    old_t = current_timeout[0]
                    if median_lat > old_t * (1 + LATENCY_TOL) and old_t < MAX_TIMEOUT:
                        current_timeout[0] = min(old_t + TIMEOUT_STEP, MAX_TIMEOUT)
                        logger.info(f"[AdaptiveTimeout] Latenz ↑ Median={median_lat:.2f}s → Timeout {old_t:.0f}s → {current_timeout[0]:.0f}s")
                    elif median_lat < old_t * (1 - LATENCY_TOL) and old_t > MIN_TIMEOUT:
                        current_timeout[0] = max(old_t - TIMEOUT_STEP, MIN_TIMEOUT)
                        logger.info(f"[AdaptiveTimeout] Latenz ↓ Median={median_lat:.2f}s → Timeout {old_t:.0f}s → {current_timeout[0]:.0f}s")
                    else:
                        logger.debug(f"[AdaptiveTimeout] Latenz stabil Median={median_lat:.2f}s → Timeout bleibt {old_t:.0f}s")
                    timeout_snapshots.append(current_timeout[0])

    def throttled_download(url_with_index: tuple[str, int]) -> dict[str, str] | None:
        """Wrapper: belegt Semaphore-Slot, liest aktuellen Timeout, führt Download durch."""
        semaphore.acquire()
        try:
            with lock:
                t = current_timeout[0] or None
            t_start = time.monotonic()
            url, index = url_with_index
            target_folder = get_subfolder(folder, index)
            oai_id = parse_qs(urlparse(url).query)["identifier"][0]
            export_type = PRM["exp_type"]
            ratelimit = PRM.get("ratelimit")
            if ratelimit:
                with _rate_limit_lock:
                    wait = ratelimit - (time.monotonic() - _last_request_time[0])
                    if wait > 0:
                        time.sleep(wait)
                    _last_request_time[0] = time.monotonic()
            try:
                with safe_get(session, url, timeout=t) as resp:
                    elapsed = time.monotonic() - t_start
                    with lock:
                        latency_window.append(elapsed)
                    if resp.status_code != 200:
                        logger.critical(f"Statuscode {resp.status_code} bei {url}")
                        return {"failed_download": oai_id}
                    if errors := re.findall(r'error\scode="(.+)">(.+)</error>', resp.text):
                        logger.warning(f"Datei {url} konnte nicht geharvestet werden ('{errors[0][0]}')")
                        return {"failed_download": oai_id}
                    if is_deleted(resp.text):
                        deleted_handling = PRM.get("deleted_handling", "keep")
                        if deleted_handling == "skip":
                            logger.info(f"Gelöschter Record übersprungen: {oai_id}")
                            return {"skipped_deleted": oai_id}
                        elif deleted_handling == "folder":
                            deleted_folder = os.path.join(folder, "_deleted")
                            os.makedirs(deleted_folder, exist_ok=True)
                            save_file(oai_id, deleted_folder, resp.text, export_type)
                            logger.info(f"Gelöschter Record in _deleted/ verschoben: {oai_id}")
                            return {"deleted": oai_id}
                        else:
                            logger.info(f"Gelöschter Record gespeichert (keep): {oai_id}")
                    elif is_invalid(resp.text):
                        invalid_folder = os.path.join(folder, "_invalid")
                        os.makedirs(invalid_folder, exist_ok=True)
                        save_file(oai_id, invalid_folder, resp.text, export_type)
                        logger.warning(f"Ungültiger Record in _invalid/ verschoben: {oai_id}")
                        return {"invalid": oai_id}
                    if not save_file(oai_id, target_folder, resp.text, export_type):
                        return {"failed_download": oai_id}
            except RequestException as e:
                logger.warning(f"Datei {url} wurde aufgrund eines {type(e).__name__} übersprungen: {e}")
                return {"failed_id": oai_id}
            with lock:
                done_count[0] += 1
        finally:
            semaphore.release()

    controller_thread = threading.Thread(target=adaptive_controller, daemon=True)
    controller_thread.start()
    logger.info(
        f"[AdaptivePool] Start mit {n_workers} Workern (max. {MAX_WORKERS}), "
        f"Timeout {current_timeout[0]:.0f}s (max. {MAX_TIMEOUT}s)"
    )

    producer_thread = threading.Thread(target=producer, daemon=True)
    producer_thread.start()

    failed_oaiids: list[dict[str, str] | None] = []
    global_index = [0]

    # Initiale Gesamtzahl: bei Liste sofort bekannt, bei Pipelining ggf. noch nicht
    # (wird über total_holder nachgereicht, sobald completeListSize geparst wurde)
    initial_total = len(ids) if isinstance(ids, list) else (total_holder[0] if total_holder else None)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        with tqdm(total=initial_total, desc="Harveste...", unit="files", dynamic_ncols=True) as pbar:

            def on_future_done(future) -> None:
                """Aktualisiert Fortschrittsbalken sofort bei Abschluss eines Downloads,
                statt erst nachdem alle IDs aus der Queue gezogen wurden."""
                result = future.result()
                with lock:
                    failed_oaiids.append(result)
                    if pbar.total is None and total_holder and total_holder[0] is not None:
                        pbar.total = total_holder[0]
                    pbar.update(1)
                    pbar.set_postfix(workers=n_workers, timeout=f"{current_timeout[0]:.0f}s")

            futures = []
            while True:
                oai_id = id_queue.get()
                if oai_id is _SENTINEL:
                    break
                idx = global_index[0]
                global_index[0] += 1
                url_with_index = (
                    PRM["b_url"] + "?verb=GetRecord&metadataPrefix=" + PRM["pref"] + "&identifier=" + str(oai_id),
                    idx,
                )
                future = executor.submit(throttled_download, url_with_index)
                future.add_done_callback(on_future_done)
                futures.append(future)

            # Wartet bis alle Downloads abgeschlossen sind. Der Fortschritt läuft
            # währenddessen bereits über on_future_done, nicht erst danach.
            for future in as_completed(futures):
                pass

    producer_thread.join()
    stop_event.set()
    controller_thread.join(timeout=MEASURE_INTERVAL + 1)

    # ---- Abschluss-Statistik ----
    w_min = min(worker_snapshots)
    w_max = max(worker_snapshots)
    w_avg = sum(worker_snapshots) / len(worker_snapshots)
    t_min = min(timeout_snapshots)
    t_max = max(timeout_snapshots)
    t_avg = sum(timeout_snapshots) / len(timeout_snapshots)
    print_and_log(
        f"[AdaptivePool] Worker-Statistik:  min. {w_min}, max. {w_max}, Ø {w_avg:.1f} Threads\n"
        f"[AdaptiveTimeout] Timeout-Statistik: min. {t_min:.0f}s, max. {t_max:.0f}s, Ø {t_avg:.1f}s",
        logger, "info",
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
    skipped_deleted = failed_files.get("skipped_deleted", [])
    moved_deleted   = failed_files.get("deleted", [])
    moved_invalid   = failed_files.get("invalid", [])
    if skipped_deleted:
        print_and_log(f"{INFO}{len(skipped_deleted)} gelöschte Records übersprungen.", logger, "info")
    if moved_deleted:
        print_and_log(f"{INFO}{len(moved_deleted)} gelöschte Records in _deleted/ verschoben.", logger, "info")
    if moved_invalid:
        print_and_log(f"{INFO}{len(moved_invalid)} ungültige Records in _invalid/ verschoben.", logger, "info")

    if stats is not None:
        total = len(failed_oaiids)
        successful = total - len(failed_download) - len(failed_ids) - len(skipped_deleted) - len(moved_deleted) - len(moved_invalid)
        stats.update(
            {
                "total": total,
                "successful": successful,
                "failed_download": len(failed_download),
                "failed_ids": len(failed_ids),
                "skipped_deleted": len(skipped_deleted),
                "moved_deleted": len(moved_deleted),
                "moved_invalid": len(moved_invalid),
            }
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
    file = os.path.join(folder, f"_ometha_{type}_ids.yaml")

    doc = {
        "Information": f"Liste erzeugt mit Ometha {__version__}",
        "date": TIMESTR,
        **_build_yaml_doc(PRM),
    }
    with open(file, "w", encoding="utf-8") as f:
        yaml.safe_dump(doc, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        f.write("ids:\n")
        f.write("\n".join([f"- '{fid}'" for fid in ids]))
    return file


def write_summary_xml(
    PRM: dict[str, Any],
    folder: str,
    stats: dict[str, int],
    runtime: str,
) -> str:
    """Schreibt eine strukturierte XML-Zusammenfassung des Harvesting-Laufs.

    Gedacht für die Weiterverarbeitung z. B. in einer eXist-db: enthält
    Quellparameter (Base-URL, Prefix, Sets, Zeitraum), Konfiguration
    (Datengeber, Exportformat, Gentle Mode, Umgang mit gelöschten Records)
    sowie die Ergebniszahlen des Laufs.

    Args:
        PRM: Parameters dictionary with harvesting configuration.
        folder: Output folder in which the summary file is written.
        stats: Dict with run counts, as populated by harvest_files().
        runtime: Formatted runtime string (HH:MM:SS.mmm).

    Returns:
        The path of the written summary XML file.
    """
    root = etree.Element("harvestSummary")

    meta = etree.SubElement(root, "meta")
    etree.SubElement(meta, "ometha", version=__version__)
    etree.SubElement(meta, "timestamp").text = OAITIMESTR
    etree.SubElement(meta, "runtime").text = runtime

    source = etree.SubElement(root, "source")
    etree.SubElement(source, "baseUrl").text = PRM.get("b_url") or ""
    etree.SubElement(source, "metadataPrefix").text = PRM.get("pref") or ""
    sets_el = etree.SubElement(source, "sets")
    sets_prm = PRM.get("sets") or []
    if sets_prm and isinstance(sets_prm, list):
        sets_dict = sets_prm[0] if sets_prm else {}
        for set_spec in sets_dict.get("additive", []):
            etree.SubElement(sets_el, "set", type="additive").text = set_spec
        for set_spec in sets_dict.get("intersection", []):
            etree.SubElement(sets_el, "set", type="intersection").text = set_spec
    if PRM.get("f_date"):
        etree.SubElement(source, "fromDate").text = str(PRM["f_date"])
    if PRM.get("u_date"):
        etree.SubElement(source, "untilDate").text = str(PRM["u_date"])

    config = etree.SubElement(root, "config")
    etree.SubElement(config, "datengeber").text = PRM.get("dat_geb") or ""
    etree.SubElement(config, "exportType").text = PRM.get("exp_type") or ""
    etree.SubElement(config, "deletedHandling").text = PRM.get("deleted_handling") or "keep"
    etree.SubElement(config, "gentle").text = "true" if PRM.get("gentle") else "false"

    results = etree.SubElement(root, "results")
    for key in (
        "total",
        "successful",
        "failed_download",
        "failed_ids",
        "skipped_deleted",
        "moved_deleted",
        "moved_invalid",
    ):
        tag = "".join(part.capitalize() if i else part for i, part in enumerate(key.split("_")))
        etree.SubElement(results, tag).text = str(stats.get(key, 0))

    file = os.path.join(folder, f"_ometha_{PRM.get('dat_geb', TIMESTR)}_summary.xml")
    tree = etree.ElementTree(root)
    tree.write(file, xml_declaration=True, encoding="UTF-8", pretty_print=True)
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