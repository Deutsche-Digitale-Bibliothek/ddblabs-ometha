import re
import sys
import time

from colorama import Fore, Style
from loguru import logger
from lxml import etree
from requests.exceptions import ConnectionError, HTTPError, RetryError, Timeout

# define global variables reused throughout the code
SEP_LINE = "--------------------------------------\n"
ACHTUNG = f"{Fore.YELLOW}Achtung:\n {Fore.WHITE}"
FEHLER = f"{Fore.RED}Fehler:\n  {Style.DIM}"
INFO = f"{Fore.YELLOW}Information: {Fore.WHITE}"
TIMESTR = time.strftime("%Y-%m-%dT%H:%M:%SZ")
NAMESPACE = "{http://www.openarchives.org/OAI/2.0/}"
ISODATEREGEX = "(?:19|20)[0-9]{2}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-9])|(?:(?!02)(?:0[1-9]|1[0-2])-(?:30))|(?:(?:0[13578]|1[02])-31))"
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
    "res_tok": None,  # oai resumption token: str
    "conf_f": None,  # Configfile: path
    "conf_m": None,  # Configmode: bool
    "auto_m": None,  # Automode: bool
    "out_f": None,  # outputfile: path
    "n_procs": None,  # number of parallel downloads: int
    "mode": None,  # mode: str "ui" or "cli"
    "exp_type": None,  # export type either "xml" or "json"
}


def print_and_log(message, logger, type: str, end="\n"):
    print(message, end)
    for placeholder in [SEP_LINE, ACHTUNG, INFO, FEHLER]:
        if placeholder in message:
            message = message.replace(placeholder, "")
    if type == "info":
        logger.info(message)
    elif type == "warning":
        logger.warning(message)


def handle_error(e, mode, url=None):
    error_messages = {
        Timeout: "Timeout determining list size. The API is not reachable.",
        RetryError: "Identifier harvesting aborted due to too many retries. Is the API reachable?",
        HTTPError: "Identifier harvesting aborted due to an HTTP error.",
    }
    if type(e) is ConnectionError:
        if "404" in str(e):
            log_critical_and_print_and_exit(
                "The API is not reachable. Is the URL correct?", mode
            )
        elif errors := re.findall(r"error\scode=['\"](.+)['\"]>(.*)<\\error", str(e)):
            log_critical_and_print_and_exit(
                f"{FEHLER} API error: {errors[0][0]}/{errors[0][1]} at {url}", mode
            )
    elif type(e) in error_messages:
        log_critical_and_print_and_exit(error_messages[type(e)], mode, e)
    else:
        log_critical_and_print_and_exit("An unexpected error occurred.", mode, e)


def log_critical_and_print_and_exit(message, mode=None, exception=None):
    print(message)
    logger.critical(message)
    if exception:
        logger.exception("Exception details:", exc_info=exception)
    if mode == "ui" and input(f"{message}\nDrücken Sie Enter zum Beenden..."):
        sys.exit()


def isinvalid_xml_content(response, url, mode):
    try:
        root = etree.XML(response.content)
    except etree.XMLSyntaxError as e:
        log_critical_and_print_and_exit(
            f"XML Syntax Error in the API response, probably no valid XML ({url}): '{e}'",
            mode,
        )
    return root
