import argparse
import os
import re
import time
from urllib.parse import parse_qs, urlparse

from ._version import __version__
from .harvester import read_yaml_file
from .helpers import ISODATEREGEX, PRM, SEP_LINE, TIMESTR


def parseargs() -> dict:
    """Parse command line arguments.
    returns: dict
    """

    def add_common_args(s):
        s.add_argument(
            "--parallel",
            "-p",
            type=int,
            help="Number of parallel downloads (default: 16)",
            default=16,
        )
        s.add_argument(
            "--timeout",
            "-t",
            type=int,
            help="Timeout between requests in seconds (default: 0)",
            default=0,
        )
        s.add_argument(
            "--outputfolder", "-o", help="Output folder", default=os.getcwd()
        )
        s.add_argument(
            "--debug",
            dest="debug",
            help="Print ListIdentifiers output",
            action="store_true",
        )
        s.add_argument(
            "--exporttype", "-e", help="Export format (xml/json)", default="xml"
        )

    def convert_common_args(args):
        PRM["timeout"], PRM["n_procs"], PRM["out_f"], PRM["debug"], PRM["exp_type"] = (
            args.timeout,
            args.parallel,
            args.outputfolder,
            args.debug,
            args.exporttype,
        )

    subparsers = {
        "conf": "Harvesting using configuration file.",
        "auto": "Automatic mode to harvest a specific URL.",
        "ids": "Harvesting using ID file.",
        "default": "Harvesting with ListIdentifiers and GetRecord verbs.",
    }
    # top-level parser
    toplevelparser = argparse.ArgumentParser(
        description="A OAI-PMH Harvester using ListIdentifiers and GetRecord verbs."
    )
    # add a --version flag
    toplevelparser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"Ometha {__version__}",
        help="Print version and exit",
    )
    # create one subparser
    subprs = toplevelparser.add_subparsers(dest="command")
    for cmd, help_text in subparsers.items():
        # iterate over subparsers to add subparser for each command
        prs = subprs.add_parser(cmd, help=help_text)
        if cmd == "default":
            add_common_args(prs)
            prs.add_argument(
                "--resumptiontoken",
                help="Resume harvesting with resumptionToken. Requires -d option.",
            )
            prs.add_argument(
                "--baseurl", "-b", required=True, help="URL of the OAI interface"
            )
            prs.add_argument(
                "--metadataprefix", "-m", required=True, help="Metadata Prefix"
            )
            prs.add_argument(
                "--datengeber", "-d", default=TIMESTR, help="Datengeber (Folder name)"
            )
            prs.add_argument(
                "--set",
                "-s",
                nargs="+",
                type=parse_set_values,
                help="Set(s) on the OAI interface, separated by ',' or '/' symbol",
            )
            prs.add_argument(
                "--fromdate",
                "-f",
                help="ISO8601 timestamp (YYYY-MM-DD), harvesting from this date onwards",
            )
            prs.add_argument(
                "--untildate",
                "-u",
                help="ISO8601 timestamp (YYYY-MM-DD), harvesting until this date",
            )
        elif cmd == "conf":
            prs.add_argument("--exporttype", "-e", help="Export format (lido/json)")
            prs.add_argument(
                "--debug",
                dest="debug",
                help="Print ListIdentifiers output",
                action="store_true",
            )
            prs.add_argument(
                "--conf",
                "-c",
                required=True,
                help="Relative or absolute path to the YAML configuration file",
            )
            prs.add_argument(
                "--auto",
                "-a",
                action="store_true",
                help="Automatic mode to harvest the period from from-date to today. Automatically adjusts the data in the configuration file.",
            )
        elif cmd == "auto":
            add_common_args(prs)
            prs.add_argument("--url", "-u", required=True, help="URL")
        elif cmd == "ids":
            add_common_args(prs)
            prs.add_argument(
                "--idfile", "-i", required=True, help="Path to ID YAML File"
            )
            prs.add_argument(
                "--datengeber", "-d", default=TIMESTR, help="Datengeber (Folder name)"
            )

    args = toplevelparser.parse_args()

    if args.command == "default":
        convert_common_args(args)
        PRM["res_tok"], PRM["b_url"], PRM["pref"], PRM["dat_geb"], PRM["sets"] = (
            args.resumptiontoken,
            args.baseurl,
            args.metadataprefix,
            args.datengeber,
            args.set,
        )
        PRM["f_date"] = (
            args.fromdate
            if args.fromdate and re.match(ISODATEREGEX, str(args.fromdate))
            else None
        )
        PRM["u_date"] = (
            args.untildate
            if args.untildate and re.match(ISODATEREGEX, str(args.untildate))
            else None
        )
        print(
            f"{SEP_LINE}{args.untildate} ist kein valides ISO8601 Date."
        ) if args.fromdate and not PRM["f_date"] else None
        print(
            f"{SEP_LINE}{args.fromdate} ist kein valides ISO8601 Date."
        ) if args.untildate and not PRM["u_date"] else None
    elif args.command == "conf":
        PRM["conf_m"] = True
        PRM["conf_f"], PRM["auto_m"], PRM["exp_type"] = (
            args.conf,
            args.auto,
            args.exporttype,
        )
        (
            PRM["b_url"],
            PRM["sets"],
            PRM["pref"],
            PRM["dat_geb"],
            PRM["timeout"],
            PRM["debug"],
        ) = read_yaml_file(
            PRM["conf_f"],
            ["baseurl", "set", "metadataPrefix", "datengeber", "timeout", "debug"],
        )
        # outputfolder: if none is defined use the current working directory
        PRM["out_f"] = read_yaml_file(PRM["conf_f"], ["outputfolder"], os.getcwd())[0]
        # n_procs is not given in the config file, use default value
        PRM["n_procs"] = 16
        PRM["b_url"], PRM["pref"] = (
            re.sub(r"/\s$", "", PRM["b_url"]),
            re.sub(r"\s$", "", PRM["pref"]),
        )
    elif args.command == "auto":
        convert_common_args(args)
        PRM["auto_m"] = True
        url = urlparse(args.url)
        PRM["b_url"] = f"{url.scheme}://{url.netloc}{url.path}"
        params = parse_qs(url.query, keep_blank_values=False)
        PRM["pref"], PRM["sets"], PRM["f_date"], PRM["u_date"] = (
            params.get("metadataPrefix", [None])[0],
            params.get("set", None),
            params.get("from", None),
            params.get("until", None),
        )
        PRM["dat_geb"] = time.strftime("%Y%m%d%H%M%S")
    elif args.command == "ids":
        convert_common_args(args)
        PRM["b_url"], PRM["sets"], PRM["pref"] = read_yaml_file(
            args.idfile, ["url", "sets", "mprefix"]
        )
        if PRM["sets"] == ["komplett"]:
            PRM["sets"] = None
        else:
            PRM["sets"] = [{"additive": PRM["sets"], "intersection": []}]
        PRM["dat_geb"], PRM["id_f"] = args.datengeber, args.idfile

    return PRM


def parse_set_values(value):
    sets = {"additive": [], "intersection": []}
    if "/" in value:
        slash_parts = value.split("/")
        sets["additive"].extend(item.strip() for item in slash_parts[0].split(","))
        sets["intersection"].extend(item.strip() for item in slash_parts[1].split(","))
    elif "," in value:
        sets["additive"].extend(item.strip() for item in value.split(","))
    else:
        sets["additive"].append(value.strip())  # If no separator is found, assume comma

    return sets
