import argparse
import os
import re
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

from ._version import __version__
from .harvester import read_yaml_file
from .helpers import ISODATEREGEX, OAITIMESTR, PRM, SEP_LINE, TIMESTR, parse_natural_date


def _resolve_date(value: str | None) -> str | None:
    """Löst einen Datumswert auf – entweder ISO8601 oder natürlichsprachig (z. B. ``1d``)."""
    if not value:
        return None
    if re.match(ISODATEREGEX, str(value)):
        return str(value)
    return parse_natural_date(str(value))


def parseargs() -> dict[str, Any]:
    """Parse command-line arguments and return a populated PRM dictionary.

    Supports four subcommands: ``default`` (full ListIdentifiers/GetRecord flow),
    ``conf`` (YAML config file), ``auto`` (full URL with embedded parameters), and
    ``ids`` (harvest from a pre-built ID file).

    Returns:
        A PRM dictionary with all harvesting parameters populated from CLI arguments.
    """

    def add_common_args(s):
        s.add_argument(
            "--parallel",
            "-p",
            type=int,
            help="Number of parallel downloads (default: auto)",
            default=None,
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
            prs.add_argument(
                "--no-log",
                dest="no_log",
                action="store_true",
                help="Kein Logfile anlegen (sinnvoll für Cron-Betrieb mit externem Logging).",
            )
            prs.add_argument(
                "--cleanup-on-empty",
                dest="cleanup_empty",
                action="store_true",
                help="Ausgabeordner löschen wenn keine Datensätze geharvestet wurden.",
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
        PRM["f_date"] = _resolve_date(args.fromdate)
        PRM["u_date"] = _resolve_date(args.untildate)
        if args.fromdate and not PRM["f_date"]:
            print(f"{SEP_LINE}{args.fromdate} ist kein valides ISO8601 Date.")
        if args.untildate and not PRM["u_date"]:
            print(f"{SEP_LINE}{args.untildate} ist kein valides ISO8601 Date.")
    elif args.command == "conf":
        PRM["conf_m"] = True
        PRM["conf_f"], PRM["auto_m"], PRM["exp_type"] = (
            args.conf,
            args.auto,
            args.exporttype,
        )
        PRM["no_log"] = args.no_log
        PRM["cleanup_empty"] = args.cleanup_empty
        (
            PRM["b_url"],
            PRM["sets"],  # Ensure this is a dict
            PRM["pref"],
            PRM["dat_geb"],
            PRM["timeout"],
            PRM["debug"],
        ) = read_yaml_file(
            PRM["conf_f"],
            ["baseurl", "sets", "metadataPrefix", "datengeber", "timeout", "debug"],
        )
        # Datumsgrenzen aus Konfigurationsdatei lesen
        f_date_raw, u_date_raw = read_yaml_file(
            PRM["conf_f"], ["from-Datum", "until-Datum"]
        )
        PRM["f_date"] = _resolve_date(str(f_date_raw)) if f_date_raw else None
        PRM["u_date"] = _resolve_date(str(u_date_raw)) if u_date_raw else None

        # If baseurl is not found, try 'url' as a fallback
        if PRM["b_url"] is None:
            PRM["b_url"] = read_yaml_file(PRM["conf_f"], ["url"])[0]

        # If metadataPrefix is not found, try 'mprefix' as a fallback
        if PRM["pref"] is None:
            PRM["pref"] = read_yaml_file(PRM["conf_f"], ["mprefix"])[0]

        # If datengeber is not found, try 'name' as a fallback
        if PRM["dat_geb"] is None:
            PRM["dat_geb"] = read_yaml_file(PRM["conf_f"], ["name"])[0]

        # outputfolder: if none is defined use the current working directory
        PRM["out_f"] = read_yaml_file(PRM["conf_f"], ["outputfolder"], os.getcwd())[0]
        # n_procs: read from config file, None means auto-scale based on ID count
        PRM["n_procs"] = read_yaml_file(PRM["conf_f"], ["numberofprocesses"])[0]
        # Clean up base URL and prefix if they are not None
        if PRM["b_url"] is not None:
            PRM["b_url"] = re.sub(r"/\s$", "", PRM["b_url"])
        if PRM["pref"] is not None:
            PRM["pref"] = re.sub(r"\s$", "", PRM["pref"])

        # Convert sets to a dictionary if it's a list
        if isinstance(PRM["sets"], list):
            # Handle backwards compatibility: treat "komplett" as no sets selected
            filtered_sets = [s for s in PRM["sets"] if s.lower() != "komplett"]
            PRM["sets"] = [
                {
                    "additive": filtered_sets,
                    "intersection": [],
                }
            ]  # Wrap in a list to maintain expected format
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
            args.idfile, ["baseurl", "sets", "metadataPrefix"]
        )
        if PRM["sets"] == ["komplett"]:
            PRM["sets"] = None
        else:
            PRM["sets"] = [{"additive": PRM["sets"], "intersection": []}]
        PRM["dat_geb"], PRM["id_f"] = args.datengeber, args.idfile

    return PRM


def parse_set_values(value: str | None) -> dict[str, list[str]]:
    """Parse a set specification string into additive and intersection components.

    A ``/`` separator splits additive sets (left) from intersection sets (right);
    ``,`` separates individual set names within each part. A plain value without
    separators is placed in ``"additive"``.

    Args:
        value: Set specification string, or ``None`` for an empty result.

    Returns:
        A dict with ``"additive"`` and ``"intersection"`` keys, each a list of set names.
    """
    if value is not None:
        sets = {"additive": [], "intersection": []}
        if "/" in value:
            slash_parts = value.split("/")
            sets["additive"].extend(item.strip() for item in slash_parts[0].split(","))
            sets["intersection"].extend(
                item.strip() for item in slash_parts[1].split(",")
            )
        elif "," in value:
            sets["additive"].extend(item.strip() for item in value.split(","))
        else:
            sets["additive"].append(
                value.strip()
            )  # If no separator is found, assume comma
    else:
        sets = {"additive": [], "intersection": []}
    return sets
