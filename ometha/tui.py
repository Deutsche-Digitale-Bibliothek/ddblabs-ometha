import os
import re
import sys
from typing import Any
from urllib.parse import quote_plus

from requests import Session
from requests.exceptions import ConnectionError, HTTPError, Timeout

from .cli import parse_set_values
from .helpers import resolve_date, safe_get
from .i18n import t
from .harvester import (
    NAMESPACE,
    PRM,
    SEP_LINE,
    TIMESTR,
    handle_error,
    isinvalid_xml_content,
    log_critical_and_print_and_exit,
)


def interactiveMode(session: Session) -> dict[str, Any]:
    """Run the interactive (TUI) mode to collect harvesting parameters from user input.

    Presents a menu for normal harvesting, ID-file harvesting, resumption token
    continuation, set listing, or exit. Populates and returns the PRM dictionary.

    Args:
        session: HTTP session used for live API lookups (e.g. listing sets).

    Returns:
        A populated PRM dictionary with all parameters entered by the user.
    """

    PRM["mode"] = "ui"

    def get_valid_input(prompt, validator, error_message):
        while True:
            user_input = input(prompt).strip()
            if validator(user_input):
                return user_input
            print(error_message)

    def get_folder_input(prompt, default_folder):
        folder_input = input(prompt).strip()
        return os.path.expanduser(folder_input) if folder_input else default_folder

    def is_valid_url(url):
        return bool(re.search(r"https?://\S+", url))

    how_to_start_input = (
        input(
            t("tui.start_menu")
        )
        .strip()
        .upper()
    )

    # If no input is provided, default to "N"
    how_to_start = how_to_start_input if how_to_start_input else "N"

    # Validate the input
    while how_to_start not in {"N", "I", "R", "S", "E"}:
        print(t("tui.invalid_option"))
        how_to_start_input = (
            input(
                t("tui.start_menu")
            )
            .strip()
            .upper()
        )
        how_to_start = how_to_start_input if how_to_start_input else "N"

    def add_common_args():
        PRM["dat_geb"] = input(t("tui.prompt_datengeber")) or TIMESTR
        PRM["out_f"] = get_folder_input(t("tui.prompt_folder"), os.getcwd())
        gentle_input = get_valid_input(
            t("tui.prompt_gentle"),
            lambda x: x.lower() in {"y", "n", "yes", "no", "j", "ja", "nein", ""},
            t("tui.gentle_invalid"),
        ).lower()
        PRM["gentle"] = gentle_input in {"y", "yes", "j", "ja"}
        from .helpers import GENTLE_DEFAULTS
        if PRM["gentle"]:
            n_procs_input = input(t("tui.prompt_parallel_gentle", default=GENTLE_DEFAULTS['n_procs'])).strip()
            PRM["n_procs"] = int(n_procs_input) if n_procs_input else GENTLE_DEFAULTS["n_procs"]
        else:
            n_procs_input = input(t("tui.prompt_parallel_auto")).strip()
            PRM["n_procs"] = int(n_procs_input) if n_procs_input else None
        batch_size_input = input(t("tui.prompt_batchsize")).strip()
        PRM["batch_size"] = int(batch_size_input) if batch_size_input else None
        PRM["exp_type"] = (
            get_valid_input(
                t("tui.prompt_exporttype"),
                lambda x: x.lower() in {"xml", "json"} or x == "",
                t("tui.invalid_format"),
            )
            or "xml"
        )
        if PRM["gentle"]:
            timeout_raw = input(t("tui.prompt_timeout_gentle", default=GENTLE_DEFAULTS['timeout'])).strip()
            PRM["timeout"] = float(timeout_raw) if timeout_raw else GENTLE_DEFAULTS["timeout"]
            ratelimit_raw = input(t("tui.prompt_ratelimit_gentle", default=GENTLE_DEFAULTS['ratelimit'])).strip()
            PRM["ratelimit"] = float(ratelimit_raw) if ratelimit_raw else GENTLE_DEFAULTS["ratelimit"]
        else:
            timeout_raw = input(t("tui.prompt_timeout")).strip()
            PRM["timeout"] = float(timeout_raw) if timeout_raw else 20
            ratelimit_raw = input(t("tui.prompt_ratelimit")).strip()
            PRM["ratelimit"] = float(ratelimit_raw) if ratelimit_raw else None
        deleted_input = get_valid_input(
            t("tui.prompt_deleted"),
            lambda x: x.lower() in {"keep", "skip", "folder"} or x == "",
            t("tui.deleted_invalid"),
        ).lower()
        PRM["deleted_handling"] = deleted_input or "keep"

    if how_to_start == "E":
        sys.exit(t("tui.program_ended"))
    elif how_to_start == "S":
        PRM["b_url"] = get_valid_input(t("tui.prompt_baseurl"), is_valid_url, t("tui.invalid_url"))
        get_sets_mprefs(PRM["b_url"] + "?verb=ListSets", session, sets={})
    elif how_to_start == "N":
        add_common_args()
        PRM["b_url"] = get_valid_input(t("tui.prompt_baseurl"), is_valid_url, t("tui.invalid_url"))
        PRM["pref"] = get_valid_input(
            t("tui.prompt_prefix"),
            lambda prefix: bool(prefix),
            t("tui.field_empty"),
        )
        # if the user doesnt enter anything, set to "all"
        PRM["sets"] = [
            parse_set_values(
                input(t("tui.prompt_sets"))
                or None
            )
        ]
        f_date_raw = input(t("tui.prompt_fromdate")) or None
        u_date_raw = input(t("tui.prompt_untildate")) or None
        PRM["f_date"] = resolve_date(f_date_raw)
        PRM["u_date"] = resolve_date(u_date_raw)
        if f_date_raw and not PRM["f_date"]:
            sys.exit(t("tui.invalid_date", value=f_date_raw))
        if u_date_raw and not PRM["u_date"]:
            sys.exit(t("tui.invalid_date", value=u_date_raw))
    if how_to_start == "I":
        add_common_args()
        PRM["id_f"] = get_valid_input(
            t("tui.prompt_idfile"),
            lambda file: os.path.exists(file),
            t("tui.invalid_idfile"),
        )
        try:
            ymlfile = open(PRM["id_f"], "r", encoding="utf-8")
            lines = ymlfile.read().splitlines()[2:5]
            PRM["b_url"] = re.sub(r"/\s$", "", lines[0].split(": ")[1])
            PRM["sets"], PRM["pref"] = lines[1].split(": ")[1], lines[2].split(": ")[1]
        except Exception:
            sys.exit(t("tui.yaml_load_failed"))
    elif how_to_start == "R":
        add_common_args()
        PRM["b_url"] = get_valid_input(t("tui.prompt_baseurl"), is_valid_url, t("tui.invalid_url"))
        PRM["pref"] = get_valid_input(
            t("tui.prompt_prefix"),
            lambda prefix: bool(prefix),
            t("tui.field_empty"),
        )
        PRM["res_tok"] = get_valid_input(
            t("tui.prompt_resumption_token"),
            lambda token: bool(token),
            t("tui.token_empty"),
        )

    return PRM


def get_sets_mprefs(url: str, session: Session, sets: dict[str, str]) -> None:
    """Fetch and display all OAI sets available at the given ListSets URL.

    Follows resumption tokens across pages to collect all sets, prints them sorted
    by name, then exits. Also prints available metadata prefixes.

    Args:
        url: The ListSets URL to query.
        session: HTTP session to use for requests.
        sets: Accumulator dict mapping set name to set spec, shared across recursive calls.
    """
    try:
        response = safe_get(session, url, timeout=(20, 80))
        root = isinvalid_xml_content(response, url, mode="ui")
    except (Timeout, HTTPError, ConnectionError) as e:
        handle_error(e, "ui", url)

    # get Sets
    for s in root.findall(f".//{NAMESPACE}set"):
        name, spec = (
            s.findtext(f".//{NAMESPACE}setName"),
            s.findtext(f".//{NAMESPACE}setSpec"),
        )
        if name in sets:
            print(t("tui.set_duplicate", name=name))
        else:
            sets[name] = spec
    # more pages: the recursive call prints everything and exits
    resumption_tokens = root.findall(f".//{NAMESPACE}resumptionToken")
    if resumption_tokens and resumption_tokens[0].text:
        token = quote_plus(resumption_tokens[0].text)
        get_sets_mprefs(f"{url.split('&resumptionToken=')[0]}&resumptionToken={token}", session, sets)
        return

    if len(sets) == 0:
        log_critical_and_print_and_exit(f"{SEP_LINE}{t('tui.no_sets')}", mode="ui")
    print(f"{SEP_LINE}{t('tui.sets_found')}\n{SEP_LINE}")
    for key in sorted(sets.keys(), key=lambda x: x.lower()):
        print(f"{key} [{sets[key]}]\n{SEP_LINE[:-1]}")

    # valid metadata prefixes come from ListMetadataFormats, not ListSets
    formats_url = f"{url.split('?')[0]}?verb=ListMetadataFormats"
    try:
        formats_root = isinvalid_xml_content(safe_get(session, formats_url, timeout=(20, 80)), formats_url, mode="ui")
        prefixes = sorted(p.text for p in formats_root.findall(f".//{NAMESPACE}metadataPrefix"))
    except (Timeout, HTTPError, ConnectionError):
        prefixes = []
    if prefixes:
        print(f"{SEP_LINE}{t('tui.prefixes_found', url=formats_url)}\n{SEP_LINE}{', '.join(prefixes)}\n{SEP_LINE[:-1]}")
    sys.exit(0)