import time

from colorama import Fore, Style, init

# define global variables reused throughout the code
SEP_LINE = "--------------------------------------\n"
ACHTUNG = f"{Fore.YELLOW}Achtung:\n {Fore.WHITE}"
FEHLER = f"{Fore.RED}Fehler:\n  {Style.DIM}"
INFO = f"{Fore.YELLOW}Information: {Fore.WHITE}"
TIMESTR = time.strftime("%Y-%m-%dT%H:%M:%SZ")
NAMESPACE = "{http://www.openarchives.org/OAI/2.0/}"
ISODATEREGEX = "(?:19|20)[0-9]{2}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-9])|(?:(?!02)(?:0[1-9]|1[0-2])-(?:30))|(?:(?:0[13578]|1[02])-31))"
URLREGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
# TODO read the version from setup.py
__version__ = "2.0.0"
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
