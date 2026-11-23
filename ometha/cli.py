import argparse
import os
import re
import sys
import time
from typing import Any
from urllib.parse import parse_qs, urlparse

from ._version import __version__
from .harvester import read_yaml_file
from .helpers import PRM, SEP_LINE, TIMESTR, resolve_date
from .i18n import t

_resolve_date = resolve_date


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
            help=t("cli.help_parallel"),
            default=None,
        )
        s.add_argument(
            "--timeout",
            "-t",
            type=int,
            help=t("cli.help_timeout"),
            default=None,
        )
        s.add_argument(
            "--maxtimeout",
            type=int,
            help=t("cli.help_maxtimeout"),
            default=None,
            dest="max_timeout",
        )
        s.add_argument(
            "--batchsize",
            type=int,
            help=t("cli.help_batchsize"),
            default=None,
            dest="batch_size",
        )
        s.add_argument("--outputfolder", "-o", help=t("cli.help_outputfolder"), default=os.getcwd())
        s.add_argument(
            "--debug",
            dest="debug",
            help=t("cli.help_debug"),
            action="store_true",
        )
        s.add_argument("--exporttype", "-e", help=t("cli.help_exporttype"), default="xml")
        s.add_argument(
            "--gentle",
            dest="gentle",
            action="store_true",
            default=False,
            help=t("cli.help_gentle"),
        )
        s.add_argument(
            "--ratelimit",
            type=float,
            default=None,
            dest="ratelimit",
            help=t("cli.help_ratelimit"),
        )
        s.add_argument(
            "--deleted",
            dest="deleted_handling",
            choices=["keep", "skip", "folder"],
            default="keep",
            help=t("cli.help_deleted"),
        )
        s.add_argument(
            "--summary",
            dest="summary",
            action="store_true",
            default=False,
            help=t("cli.help_summary"),
        )
        s.add_argument(
            "--sort-invalid",
            dest="sort_invalid",
            action="store_true",
            default=False,
            help=t("cli.help_sort_invalid"),
        )

    def convert_common_args(args):
        from .helpers import GENTLE_DEFAULTS
        PRM["gentle"] = args.gentle
        PRM["deleted_handling"] = args.deleted_handling
        PRM["summary"], PRM["sort_invalid"] = args.summary, args.sort_invalid
        if args.gentle:
            PRM["n_procs"]   = args.parallel  if args.parallel  is not None else GENTLE_DEFAULTS["n_procs"]
            PRM["timeout"]   = args.timeout   if args.timeout   is not None else GENTLE_DEFAULTS["timeout"]
            PRM["ratelimit"] = args.ratelimit if args.ratelimit is not None else GENTLE_DEFAULTS["ratelimit"]
        else:
            PRM["n_procs"]   = args.parallel
            PRM["timeout"]   = args.timeout if args.timeout is not None else 20
            PRM["ratelimit"] = args.ratelimit
        PRM["out_f"] = os.path.expanduser(args.outputfolder) if args.outputfolder else args.outputfolder
        PRM["debug"], PRM["exp_type"], PRM["max_timeout"], PRM["batch_size"] = (
            args.debug,
            args.exporttype,
            args.max_timeout,
            args.batch_size,
        )

    subparsers = {
        "conf": t("cli.sub_conf"),
        "auto": t("cli.sub_auto"),
        "ids": t("cli.sub_ids"),
        "default": t("cli.sub_default"),
    }
    # top-level parser
    toplevelparser = argparse.ArgumentParser(
        description=t("cli.description")
    )
    # add a --version flag
    toplevelparser.add_argument(
        "--version",
        "-v",
        action="version",
        version=t("cli.version_string", version=__version__),
        help=t("cli.help_version"),
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
                help=t("cli.help_resumptiontoken"),
            )
            prs.add_argument("--baseurl", "-b", required=True, help=t("cli.help_baseurl"))
            prs.add_argument("--metadataprefix", "-m", required=True, help=t("cli.help_metadataprefix"))
            prs.add_argument("--datengeber", "-d", default=TIMESTR, help=t("cli.help_datengeber"))
            prs.add_argument(
                "--set",
                "-s",
                nargs="+",
                type=parse_set_values,
                help=t("cli.help_set"),
            )
            prs.add_argument(
                "--fromdate",
                "-f",
                help=t("cli.help_fromdate"),
            )
            prs.add_argument(
                "--untildate",
                "-u",
                help=t("cli.help_untildate"),
            )
        elif cmd == "conf":
            prs.add_argument("--exporttype", "-e", help=t("cli.help_exporttype_conf"))
            prs.add_argument(
                "--debug",
                dest="debug",
                help=t("cli.help_debug"),
                action="store_true",
            )
            prs.add_argument(
                "--conf",
                "-c",
                required=True,
                help=t("cli.help_conf"),
            )
            prs.add_argument(
                "--auto",
                "-a",
                action="store_true",
                help=t("cli.help_auto"),
            )
            prs.add_argument(
                "--no-log",
                dest="no_log",
                action="store_true",
                help=t("cli.help_no_log"),
            )
            prs.add_argument(
                "--gentle",
                dest="gentle",
                action="store_true",
                default=False,
                help=t("cli.help_gentle_conf"),
            )
            prs.add_argument(
                "--cleanup-on-empty",
                dest="cleanup_empty",
                action="store_true",
                help=t("cli.help_cleanup_on_empty"),
            )
            prs.add_argument(
                "--summary",
                dest="summary",
                action="store_true",
                default=False,
                help=t("cli.help_summary"),
            )
            prs.add_argument(
                "--sort-invalid",
                dest="sort_invalid",
                action="store_true",
                default=False,
                help=t("cli.help_sort_invalid"),
            )
        elif cmd == "auto":
            add_common_args(prs)
            prs.add_argument("--url", "-u", required=True, help=t("cli.help_url"))
        elif cmd == "ids":
            add_common_args(prs)
            prs.add_argument("--idfile", "-i", required=True, help=t("cli.help_idfile"))
            prs.add_argument("--datengeber", "-d", default=TIMESTR, help=t("cli.help_datengeber"))

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
        # "komplett" aus Sets entfernen (kein Filter = vollständiges Harvesting)
        if PRM["sets"]:
            for set_dict in PRM["sets"]:
                set_dict["additive"] = [s for s in set_dict["additive"] if s.lower() != "komplett"]
                set_dict["intersection"] = [s for s in set_dict["intersection"] if s.lower() != "komplett"]
        PRM["f_date"] = _resolve_date(args.fromdate)
        PRM["u_date"] = _resolve_date(args.untildate)
        if args.fromdate and not PRM["f_date"]:
            sys.exit(SEP_LINE + t("cli.invalid_date", value=args.fromdate))
        if args.untildate and not PRM["u_date"]:
            sys.exit(SEP_LINE + t("cli.invalid_date", value=args.untildate))
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
        # Datumsgrenzen aus Konfigurationsdatei lesen.
        # Unterstützt beide Key-Namen: "from-Datum"/"until-Datum" (Auto-Modus)
        # und "fromdate"/"untildate" (manuell gesetzt, wie in der Dokumentation beschrieben).
        f_auto, f_manual, u_auto, u_manual = read_yaml_file(
            PRM["conf_f"], ["from-Datum", "fromdate", "until-Datum", "untildate"]
        )
        # Auto-Modus schreibt "fromdate"/"untildate" → diese haben Vorrang
        f_date_raw = f_manual or f_auto
        u_date_raw = u_manual or u_auto
        PRM["f_date"] = _resolve_date(str(f_date_raw)) if f_date_raw else None
        PRM["u_date"] = _resolve_date(str(u_date_raw)) if u_date_raw else None
        if f_date_raw and not PRM["f_date"]:
            sys.exit(SEP_LINE + t("cli.invalid_date", value=f_date_raw))
        if u_date_raw and not PRM["u_date"]:
            sys.exit(SEP_LINE + t("cli.invalid_date", value=u_date_raw))

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
        PRM["out_f"] = os.path.expanduser(read_yaml_file(PRM["conf_f"], ["outputfolder"], os.getcwd())[0] or os.getcwd())
        # n_procs: read from config file, None means auto-scale based on ID count
        PRM["n_procs"] = read_yaml_file(PRM["conf_f"], ["numberofprocesses"])[0]
        # batch_size: read from config file, None means no subfolders
        PRM["batch_size"] = read_yaml_file(PRM["conf_f"], ["batchsize"])[0]
        # max_timeout: read from config file, None means default (120s)
        PRM["max_timeout"] = read_yaml_file(PRM["conf_f"], ["maxtimeout"])[0]
        # deleted_handling: read from config file, default "keep"
        PRM["deleted_handling"] = read_yaml_file(PRM["conf_f"], ["deleted"])[0] or "keep"
        # ratelimit: read from config file
        PRM["ratelimit"] = read_yaml_file(PRM["conf_f"], ["ratelimit"])[0]
        # gentle: YAML-Wert hat Vorrang vor dem CLI-Flag; fehlende Werte werden mit GENTLE_DEFAULTS gefüllt
        from .helpers import GENTLE_DEFAULTS
        gentle_yaml = read_yaml_file(PRM["conf_f"], ["gentle"])[0]
        PRM["gentle"] = bool(gentle_yaml) if gentle_yaml is not None else args.gentle
        if PRM["gentle"]:
            for key, default_key in (("n_procs", "n_procs"), ("timeout", "timeout"), ("ratelimit", "ratelimit")):
                if PRM[key] is None:
                    PRM[key] = GENTLE_DEFAULTS[default_key]
        # summary / sortinvalid: YAML-Wert hat Vorrang vor dem CLI-Flag
        summary_yaml, sort_invalid_yaml = read_yaml_file(PRM["conf_f"], ["summary", "sortinvalid"])
        PRM["summary"] = bool(summary_yaml) if summary_yaml is not None else args.summary
        PRM["sort_invalid"] = bool(sort_invalid_yaml) if sort_invalid_yaml is not None else args.sort_invalid
        # resumptiontoken: gesetzt wenn Checkpoint-YAML verwendet wird
        PRM["res_tok"] = read_yaml_file(PRM["conf_f"], ["resumptiontoken"])[0]
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
        PRM["b_url"], PRM["sets"], PRM["pref"] = read_yaml_file(args.idfile, ["baseurl", "sets", "metadataPrefix"])
        if PRM["sets"] == ["komplett"]:
            PRM["sets"] = None
        else:
            PRM["sets"] = [{"additive": PRM["sets"], "intersection": []}]
        PRM["dat_geb"], PRM["id_f"] = args.datengeber, args.idfile

    PRM["mode"] = "cli"
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
            sets["intersection"].extend(item.strip() for item in slash_parts[1].split(","))
        elif "," in value:
            sets["additive"].extend(item.strip() for item in value.split(","))
        else:
            sets["additive"].append(value.strip())  # If no separator is found, assume comma
    else:
        sets = {"additive": [], "intersection": []}
    return sets