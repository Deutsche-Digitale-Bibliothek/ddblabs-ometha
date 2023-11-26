import os
import re
import sys
from harvester import SEP_LINE, NAMESPACE, TIMESTR, PRM, print_and_exit, handle_error, isinvalid_xml_content
from requests.exceptions import HTTPError, ConnectionError, Timeout
from cli import parse_set_values
from urllib.parse import quote_plus


def interactiveMode(session):
    def get_valid_input(prompt, validator, error_message):
        while True:
            user_input = input(prompt).strip()
            if validator(user_input):
                return user_input
            print(error_message)

    def get_folder_input(prompt, default_folder):
        folder_input = input(prompt).strip()
        return folder_input if folder_input else default_folder

    def is_valid_url(url):
        return bool(re.search(r'https?://\S+', url))

    how_to_start = get_valid_input(
        "Bitte wählen Sie eine Option:\n\n- [N]ormales Harvesting\n- Harvesting von Records per [I]D-File\n- Harvesten von Identifiern fortsetzen mit einem [R]esumptionToken\n- Anzeige aller auf der Schnittstelle vorhandener [S]ets\n- Programm verlassen mit [E]xit\n => ",
        lambda option: option in {"N", "I", "R", "S", "E", "n", "i", "r", "s", "e"},
        "Die Eingabe wurde nicht verstanden. Geben Sie entweder N, I, S, R oder E ein.").upper()

    def add_common_args():
        PRM["dat_geb"] = input("Datengeber: ") or TIMESTR
        PRM["out_f"] = get_folder_input("Ordner in den geharvestet werden soll: ", os.getcwd())
        PRM["n_procs"] = int(input("Anzahl an parallelen Downloads (default: 16): ") or 16)
        PRM["exp_type"] = get_valid_input("Exportformat (xml/json): ", lambda x: x.lower() in {"xml", "json"},"Kein korrektes Format!")
        PRM["timeout"] = float(input("Timeout in Sekunden (default: 0): ") or 0)

    if how_to_start == "E":
        sys.exit("Programm beendet.")
    elif how_to_start == "S":
        PRM["b_url"] = get_valid_input("Base-URL: ", is_valid_url, "Keine valide URL!")
        get_sets_mprefs(PRM["b_url"] + "?verb=ListSets", session, sets={})
    elif how_to_start == "N":
        add_common_args()
        PRM["b_url"] = get_valid_input("Base-URL: ", is_valid_url, "Keine valide URL!")
        PRM["pref"] = get_valid_input("Metadata Prefix: ", lambda prefix: bool(prefix), "Feld darf nicht leer sein!")
        PRM["sets"] = parse_set_values(input("Set(s) kommagetrennt oder (für Schnittmenge mit '/' getrennt): ") or None)
        PRM["f_date"] = input("Fromdate: ") or None
        PRM["u_date"] = input("Untildate: ") or None
    if how_to_start == "I":
        add_common_args()
        PRM["id_f"] = get_valid_input("idfile: ", lambda file: os.path.exists(file),"Bitte geben Sie eine valide ID-Datei an.")
        try:
            ymlfile = open(PRM["id_f"], "r", encoding="utf-8")
            lines = ymlfile.read().splitlines()[2:5]
            PRM["b_url"] = re.sub(r"/\s$", "", lines[0].split(": ")[1])
            PRM["sets"], PRM["pref"] = lines[1].split(": ")[1], lines[2].split(": ")[1]
        except Exception:
            sys.exit("Konnte YAML Datei nicht laden")
    elif how_to_start == "R":
        add_common_args()
        PRM["b_url"] = get_valid_input("Base-URL: ", is_valid_url, "Keine valide URL!")
        PRM["pref"] = get_valid_input("Metadata Prefix: ", lambda prefix: bool(prefix), "Feld darf nicht leer sein!")
        PRM["res_tok"] = get_valid_input("Resumption Token: ", lambda token: bool(token),"Resumption Token darf nicht leer sein.")

    return PRM


def get_sets_mprefs(url, session, sets: dict):
    try:
        response = session.get(url, verify=False, timeout=(20, 80))
        root = isinvalid_xml_content(response, url, mode="ui")
    except (Timeout, HTTPError, ConnectionError) as e:
        handle_error(e, "ui", url)

    # get Sets
    for s in root.findall(f".//{NAMESPACE}set"):
        name, spec = s.findtext(f".//{NAMESPACE}setName"), s.findtext(f".//{NAMESPACE}setSpec")
        if name in sets:
            print(f"Set '{name}' mehrmals gefunden")
        else:
            sets[name] = spec
    if len(sets) > 0:
        print(f"{SEP_LINE}Auf der Schnittstelle sind folgende Sets vorhanden:\n{SEP_LINE}")
        for key in sorted(sets.keys(), key=lambda x: x.lower()):
            print(f"{key} [{sets[key]}]\n{SEP_LINE[:-1]}")
        sys.exit(0)
    else:
        print_and_exit(f"{SEP_LINE}Keine Sets gefunden, ist die URL korrekt?", mode="ui")

    # get valid metadata prefixes
    prefixes = sorted(prefix.text for prefix in root.findall(".//{NAMESPACE}metadataPrefix"))
    print(
        f"{SEP_LINE}Auf der Schnittstelle {url} sind folgende Metadaten-Prefixe registriert:\n{SEP_LINE}"
        f"{', '.join(prefixes) if prefixes else ''}\n{SEP_LINE[:-1]}"
    )

    # get ResumptionToken and continue looking for sets
    resumption_tokens = root.findall(f".//{NAMESPACE}resumptionToken")
    if resumption_tokens:
        token = quote_plus(resumption_tokens[0].text)
        get_sets_mprefs(f"{url}&resumptionToken={token}", session, sets)
