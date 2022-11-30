import argparse
import os
import time
import re
import yaml
import sys
import urllib
from halo import Halo
from colorama import Fore, Style

ISODATEREGEX = "(?:19|20)[0-9]{2}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1[0-9]|2[0-9])|(?:(?!02)(?:0[1-9]|1[0-2])-(?:30))|(?:(?:0[13578]|1[02])-31))"
urlregex = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"


def parseargs():

    parser = argparse.ArgumentParser(
        description="A OAI-PMH Harvester using ListIdentifiers and GetRecord verbs."
    )

    subparsers = parser.add_subparsers(dest="command")

    d_parser = subparsers.add_parser("default")

    d_parser.add_argument(
        "--resumptiontoken",
        help="Resume Harvesting with resumptionToken. When resumptionToken is passed, the -d Option has to be supplied as well.",
        required=False,
    )
    d_parser.add_argument(
        "--baseurl", "-b", type=str, help="URL der OAI Schnittstelle", required=True
    )
    d_parser.add_argument(
        "--metadataprefix", "-m", type=str, help="Metadata Prefix", required=True
    )
    d_parser.add_argument(
        "--datengeber", "-d", type=str, help="Datengeber (Ordnername)", required=False
    )
    d_parser.add_argument(
        "--set",
        "-s",
        type=str,
        nargs="+",
        help="Set(s) auf der OAI Schnittstelle - Eingabe getrennt durch ein Leerzeichen.",
        required=False,
    )
    d_parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        help="Timeout zwischen den Anfragen in Sekunden (default ist keine Wartezeit)",
        required=False,
    )
    d_parser.add_argument(
        "--fromdate",
        "-f",
        type=str,
        help="ISO8601 Zeitangabe (YYYY-MM-DD), Harvesting von OAI Records ab diesem Zeitpunkt",
        required=False,
    )
    d_parser.add_argument(
        "--untildate",
        "-u",
        type=str,
        help="ISO8601 Zeitangabe (YYYY-MM-DD), Harvesting von OAI Records bis zu diesem Zeitpunkt",
        required=False,
    )
    d_parser.add_argument(
        "--outputfolder", "-o", type=str, help="Output Ordner", required=False
    )
    d_parser.add_argument(
        "--parallel",
        "-p",
        type=int,
        help="Number of parallel downloads (default: 16)",
        required=False,
    )
    d_parser.add_argument(
        "--debug",
        dest="debug",
        help="Gibt den Return der ListIdentifiers aus",
        action="store_true",
        required=False,
    )
    d_parser.set_defaults(debug=False)

    c_parser = subparsers.add_parser("conf")

    c_parser.add_argument(
        "--conf",
        "-c",
        type=str,
        help="relativer oder absoluter Pfad zur YAML Konfigurationsdatei",
        required=True,
    )
    c_parser.add_argument(
        "--auto",
        "-a",
        help="Automatischer Modus zum Harvesten des Zeitraums vom from-date bis heute. Passt die Daten in der Konfigurationsdatei automatisch an.",
        action="store_true",
        required=False,
    )
    c_parser.add_argument(
        "--debug",
        dest="debug",
        help="Gibt den Return der ListIdentifiers aus",
        action="store_true",
        required=False,
    )

    a_parser = subparsers.add_parser("auto")

    a_parser.add_argument("--url", "-u", type=str, help="URL", required=True)
    a_parser.add_argument(
        "--parallel",
        "-p",
        type=int,
        help="Number of parallel downloads (default: 16)",
        required=False,
    )
    a_parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        help="Timeout zwischen den Anfragen in Sekunden (default ist keine Wartezeit)",
        required=False,
    )
    a_parser.add_argument(
        "--outputfolder", "-o", type=str, help="Output Ordner", required=False
    )

    i_parser = subparsers.add_parser("ids")

    i_parser.add_argument(
        "--idfile", "-i", type=str, help="Path to ID YAML File", required=True
    )
    i_parser.add_argument(
        "--parallel",
        "-p",
        type=int,
        help="Number of parallel downloads (default: 16)",
        required=False,
    )
    i_parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        help="Timeout zwischen den Anfragen in Sekunden (default ist keine Wartezeit)",
        required=False,
    )
    i_parser.add_argument(
        "--datengeber", "-d", type=str, help="Datengeber (Ordnername)", required=False
    )
    i_parser.add_argument(
        "--outputfolder", "-o", type=str, help="Output Ordner", required=False
    )
    i_parser.add_argument(
        "--debug",
        dest="debug",
        help="Gibt den Return der ListIdentifiers aus",
        action="store_true",
        required=False,
    )
    d_parser.set_defaults(debug=False)

    args = parser.parse_args()

    if args.command == "default":
        # "default" harvesting
        debug = args.debug
        if args.parallel is None:
            numberofprocesses = 16
        else:
            numberofprocesses = args.parallel
        timeout = args.timeout
        resumptiontoken = args.resumptiontoken
        baseurl = args.baseurl
        mprefix = args.metadataprefix
        if args.datengeber is None:
            datengeber = time.strftime("%Y%m%d%H%M%S")
        else:
            datengeber = args.datengeber
        if args.outputfolder is None:
            # wenn kein Outputfolder angegeben wurde, dann das aktuelle Verzeichnis nehmen
            outputfolder = os.getcwd()
        else:
            outputfolder = args.outputfolder
        oaiset = args.set
        # Wenn kein Timestamp spezifiziert ist, setze den Timpout auf 0
        if args.timeout is None:
            timeout = 0
        else:
            timeout = timeout
        # fromdate auslesen, wenn es gesetzt wurde dann testen und wenn der der Test erfolgreich ist, dann Variable setzen
        fromdatetest = args.fromdate
        if fromdatetest != None:
            if re.match(ISODATEREGEX, fromdatetest):
                fromdate = fromdatetest
            else:
                print(
                    f"--------------------------------------\n{fromdatetest} ist kein valides ISO8601 Date."
                )
                fromdate = None
        else:
            fromdate = None
        # untildate auslesen, wenn es gesetzt wurde dann testen und wenn der der Test erfolgreich ist, dann Variable setzen
        untildatetest = args.untildate
        if untildatetest != None:
            if re.match(ISODATEREGEX, untildatetest):
                untildate = untildatetest
            else:
                print(
                    f"--------------------------------------\n{untildatetest} ist kein valides ISO8601 Date."
                )
                untildate = None
        else:
            untildate = None
        configfile = configmode = idfile = automode = None

    elif args.command == "conf":
        configfile = args.conf
        # ... wenn eine config-Datei vorhanden ist:
        configmode = True
        automode = args.auto
        try:
            ymlfile = open(configfile, "r", encoding="utf-8")
        except OSError:
            print(
                f"--------------------------------------\n{Fore.RED}Fehlermeldung:\n  {Style.DIM}Die Konfigurationsdatei kann leider nicht gelesen werden."
            )
            sys.exit()
        else:
            cfg = yaml.safe_load(ymlfile)
            try:
                baseurl = re.sub(r"\/\s$", "", cfg["baseurl"])
                mprefix = re.sub(r"\s$", "", cfg["metadataPrefix"])
                oaiset = cfg["set"]
                untildate = cfg["untildate"]
                fromdate = cfg["fromdate"]
                datengeber = cfg["datengeber"]
                timeout = cfg["timeout"]
                debug = cfg["debug"]
                if cfg["numberofprocesses"] is not None:
                    numberofprocesses = cfg["numberofprocesses"]
                else:
                    numberofprocesses = 16
                if cfg["outputfolder"] is None:
                    outputfolder = os.getcwd()
                else:
                    outputfolder = cfg["outputfolder"]
                idfile = None
                resumptiontoken = None
            except KeyError as e:
                print(
                    f"--------------------------------------\nDer Eintrag für {e} fehlt in der YAML Datei {configfile}."
                )
                sys.exit()

    elif args.command == "ids":
        idfile = args.idfile
        try:
            ymlfile = open(idfile, "r", encoding="utf-8")
        except OSError:
            print(
                f"--------------------------------------\n{Fore.RED}Fehlermeldung:\n  {Style.DIM}Die ID-Datei kann leider nicht gelesen werden."
            )
            sys.exit()
        else:
            try:
                # Wir parsen das IDfile später nochmal, daher hier nur die ersten Zeilen (falls das ID File zu groß ist)
                ymlfile = open(idfile, "r", encoding="utf-8")
                lines = ymlfile.read().splitlines()[2:5]
                baseurl =  re.sub(r"\/\s$", "", lines[0].split(": ")[1])
                oaiset = lines[1].split(": ")[1]
                mprefix = lines[2].split(": ")[1]
            except:
                sys.exit("Konnte YAML Datei nicht laden")
            try:
                untildate = None
                fromdate = None
                if args.datengeber is None:
                    datengeber = time.strftime("%Y%m%d%H%M%S")
                else:
                    datengeber = args.datengeber
                timeout = args.timeout
                if args.timeout is None:
                    timeout = 0
                else:
                    timeout = timeout
                debug = False
                if args.parallel is None:
                    numberofprocesses = 16
                else:
                    numberofprocesses = args.parallel
                if args.outputfolder is None:
                    outputfolder = os.getcwd()
                else:
                    outputfolder = args.outputfolder
                resumptiontoken = configfile = None
                configmode = automode = False
            except KeyError as e:
                print(
                    f"--------------------------------------\nDer Eintrag für {e} fehlt in der YAML Datei {idfile}."
                )
                sys.exit()

    elif args.command == "auto":
        # auto modus mit -u Option:
        # python Ometha.py auto -u "https://digital.sulb.uni-saarland.de/viewer/oai?verb=ListIdentifiers&metadataPrefix=mets"
        brkndwnurl = urllib.parse.urlparse(args.url)
        baseurl = brkndwnurl.scheme + "://" + brkndwnurl.netloc + brkndwnurl.path
        params = urllib.parse.parse_qs(brkndwnurl.query, keep_blank_values=False)

        try:
            params["metadataPrefix"]
        except:
            print("MetadataPrefix kann nicht aus der URL ausgelesen werden")
        else:
            mprefix = params["metadataPrefix"][0]

        try:
            params["set"]
        except:
            oaiset = None
        else:
            oaiset = [params["set"][0]]

        try:
            params["from"]
        except:
            fromdate = None
        else:
            fromdate = params["from"][0]

        try:
            params["until"]
        except:
            untildate = None
        else:
            untildate = params["until"][0]
        # print(f"{baseurl=} {mprefix=} {oaiset=} {fromdate=} {untildate=} ")
        datengeber = time.strftime("%Y%m%d%H%M%S")

        if args.outputfolder is None:
            outputfolder = os.getcwd()
        else:
            outputfolder = args.outputfolder
        timeout = args.timeout
        if args.timeout is None:
            timeout = 0
        else:
            timeout = timeout
        idfile = resumptiontoken = configfile = None
        debug = configmode = automode = False
        if args.parallel is None:
            numberofprocesses = 16
        else:
            numberofprocesses = args.parallel

    return (
        baseurl,
        mprefix,
        datengeber,
        oaiset,
        debug,
        float(timeout),
        idfile,
        fromdate,
        untildate,
        resumptiontoken,
        configfile,
        configmode,
        automode,
        outputfolder,
        numberofprocesses,
    )
