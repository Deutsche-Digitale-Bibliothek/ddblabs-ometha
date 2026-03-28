import re
import sys
import time
from datetime import datetime, timedelta
from typing import Any

from colorama import Fore, Style
from loguru import logger
from lxml import etree
from requests import Response
from requests.exceptions import ConnectionError, HTTPError, RetryError, Timeout

# define global variables reused throughout the code
SEP_LINE = "--------------------------------------\n"
ACHTUNG = f"{Fore.YELLOW}Achtung:\n {Fore.WHITE}"
FEHLER = f"{Fore.RED}Fehler:\n  {Style.DIM}"
INFO = f"{Fore.YELLOW}Information: {Fore.WHITE}"
TIMESTR = time.strftime("%Y-%m-%d_%H_%M_%SZ")
OAITIMESTR = time.strftime("%Y-%m-%dT%H:%M:%SZ")
NAMESPACE = "{http://www.openarchives.org/OAI/2.0/}"
ISODATEREGEX = "(?:19|20)[0-9]{2}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-9])|(?:(?!02)(?:0[1-9]|1[0-2])-(?:30))|(?:(?:0[13578]|1[02])-31))"
NATURALDATEREGEX = r"^(\d+)(mo|m|h|d|w)$"
URLREGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"

# initialize all parameters in a dict shortened as PRM
PRM = {
    "b_url": None,  # base url: str
    "pref": None,  # metadata prefix: str
    "dat_geb": None,  # datengeber: int
    "sets": {},  # dict with two subdicts, either additive or subtractive with a list
    "debug": None,  # debug: bool
    "timeout": None,  # timeout: float
    "id_f": None,  # id file: path
    "f_date": None,  # from date: int
    "u_date": None,  # until date: int
    "no_log": None,  # kein Logfile anlegen: bool
    "cleanup_empty": None,  # leere Ausgabeordner löschen: bool
    "res_tok": None,  # oai resumption token: str
    "conf_f": None,  # Configfile: path
    "conf_m": None,  # Configmode: bool
    "auto_m": None,  # Automode: bool
    "out_f": None,  # outputfile: path
    "n_procs": None,  # number of parallel downloads: int
    "mode": None,  # mode: str "ui" or "cli"
    "exp_type": None,  # export type either "xml" or "json"
}


def parse_natural_date(value: str) -> str | None:
    """Wandelt einen natürlichsprachigen Datumsausdruck in ein ISO8601-Datum (YYYY-MM-DD) um.

    Unterstützte Einheiten:
      - ``20m``  → vor 20 Minuten
      - ``2h``   → vor 2 Stunden
      - ``1d``   → vor 1 Tag
      - ``3w``   → vor 3 Wochen
      - ``1mo``  → vor 1 Monat (ca. 30 Tage)

    Args:
        value: Eingabestring, z. B. ``"1d"`` oder ``"20m"``.

    Returns:
        ISO8601-Datumsstring (``YYYY-MM-DD`` oder ``YYYY-MM-DDTHH:MM:SSZ`` bei
        Minuten/Stunden) oder ``None`` wenn das Format nicht erkannt wird.
    """
    match = re.match(NATURALDATEREGEX, value.strip())
    if not match:
        return None
    n, unit = int(match.group(1)), match.group(2)
    units_to_seconds = {"m": 60, "h": 3600, "d": 86400, "w": 604800, "mo": 2592000}
    delta = timedelta(seconds=n * units_to_seconds[unit])
    result = datetime.now() - delta
    if unit in ("m", "h"):
        return result.strftime("%Y-%m-%dT%H:%M:%SZ")
    return result.strftime("%Y-%m-%d")


def resolve_date(value: str | None) -> str | None:
    """Löst einen Datumswert auf – entweder ISO8601 oder natürlichsprachig (z. B. ``1d``)."""
    if not value:
        return None
    if re.match(ISODATEREGEX, str(value)):
        return str(value)
    return parse_natural_date(str(value))


def configure_logging() -> None:
    """Configure loguru to output only ERROR-level messages to stderr."""
    logger.remove()  # Remove the default logger
    logger.add(
        sys.stderr,  # Log to standard error
        level="ERROR",  # Only log errors and above
        format="{time} {level} {message}",  # Customize the log format
        backtrace=False,  # Disable backtrace
        diagnose=False,  # Disable diagnostic information
    )


def print_and_log(message: str, logger: Any, type: str, end: str = "\n") -> None:
    """Print a message and log it, stripping ANSI prefix constants before logging.

    Args:
        message: The message to print and log.
        logger: The loguru logger instance.
        type: Log level — either ``"info"`` or ``"warning"``.
        end: String appended after the printed message. Defaults to newline.
    """
    print(message, end)
    for placeholder in [SEP_LINE, ACHTUNG, INFO, FEHLER]:
        if placeholder in message:
            message = message.replace(placeholder, "")
    if type == "info":
        logger.info(message)
    elif type == "warning":
        logger.warning(message)


def handle_error(e: Exception, mode: str | None, url: str | None = None) -> None:
    """Handle a request exception by logging a descriptive message and exiting.

    Args:
        e: The exception that was raised.
        mode: Run mode (``"ui"`` or ``"cli"``), forwarded to
            ``log_critical_and_print_and_exit``.
        url: URL that was being requested when the error occurred.
    """
    error_messages = {
        Timeout: "Timeout determining list size. The API is not reachable.",
        RetryError: "Identifier harvesting aborted due to too many retries. Is the API reachable?",
        HTTPError: "Identifier harvesting aborted due to an HTTP error.",
    }
    if type(e) is ConnectionError:
        if "404" in str(e):
            log_critical_and_print_and_exit("The API is not reachable. Is the URL correct?", mode)
        elif errors := re.findall(r"error\scode=['\"](.+)['\"]>(.*)<\\error", str(e)):
            log_critical_and_print_and_exit(f"{FEHLER} API error: {errors[0][0]}/{errors[0][1]} at {url}", mode)
    elif type(e) in error_messages:
        log_critical_and_print_and_exit(error_messages[type(e)], mode, e)
    else:
        log_critical_and_print_and_exit("An unexpected error occurred.", mode, e)


def log_critical_and_print_and_exit(
    message: str,
    mode: str | None = None,
    exception: Exception | None = None,
) -> None:
    """Print a critical error message, log it, and exit the program.

    Args:
        message: The error message to display and log.
        mode: If ``"ui"``, prompts the user to press Enter before exiting.
        exception: Optional exception logged with full traceback details.
    """
    print(message)
    logger.critical(message)
    if exception:
        logger.exception("Exception details:", exc_info=exception)
        sys.exit()
    if mode == "ui" and input(f"{message}\nDrücken Sie Enter zum Beenden..."):
        sys.exit()


def isinvalid_xml_content(response: Response, url: str, mode: str) -> etree._Element:
    """Parse and validate the XML content of an HTTP response.

    Args:
        response: The HTTP response whose content will be parsed.
        url: URL of the request, included in the error message on parse failure.
        mode: Run mode passed to ``log_critical_and_print_and_exit`` on error.

    Returns:
        The root element of the parsed XML document.

    Raises:
        SystemExit: If the response content is not valid XML.
    """
    try:
        root = etree.XML(response.content)
    except etree.XMLSyntaxError as e:
        log_critical_and_print_and_exit(
            f"XML Syntax Error in the API response, probably no valid XML ({url}): '{e}'",
            mode,
        )
    return root
