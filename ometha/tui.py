import os
import time
import re
import yaml
import sys
from colorama import Fore, Style
import requests
from typing import List, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib
from lxml import etree

URLREGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)"


def interactiveMode(session):
    userInput = "bad"
    while userInput == "bad":
        howTostart = input(
            "Bitte wählen Sie eine Option:\n\n- [N]ormales Harvesting\n- Harvesting von Records per [I]D-File\n- Harvesten von Identifiern fortsetzen mit einem [R]esumptionToken\n- Anzeige aller auf der Schnittstelle vorhandener [S]ets\n- Programm verlassen mit [E]xit\n => "
        )
        if howTostart == "N":
            userInput = "good"
            resumptiontoken = None
            idfile = None
            baseurl = input("Base-URL: ")
            validurl = re.search(URLREGEX, baseurl)
            # https://regex101.com/r/2CCthx/1
            while validurl is None:
                baseurl = input("Bitte geben Sie eine valide Base-URL ein: ")
                validurl = re.search(URLREGEX, baseurl)
            mprefix = input("Metadata Prefix: ")
            while mprefix == "":
                mprefix = input("Metadata Prefix: ")
            datengeber = input("Datengeber: ")
            if datengeber == "":
                datengeber = time.strftime("%Y%m%d%H%M%S")
            outputfolder = input("Ordner in den geharvestet werden soll: ")
            if outputfolder == "":
                outputfolder = os.getcwd()
            oaiset = input("Set(s) kommagetrennt ohne Leerzeichen: ")
            if oaiset == "":
                oaiset = None
            else:
                oaiset = oaiset.split(",")
            fromdate = input("Fromdate: ")
            if fromdate == "":
                fromdate = None
            untildate = input("Untildate: ")
            if untildate == "":
                untildate = None
            numberofprocesses = input("Anzahl an parallelen Downloads (default: 16): ")
            if numberofprocesses == "":
                numberofprocesses = 16
            else:
                numberofprocesses = int(numberofprocesses)
        elif howTostart == "I":
            userInput = "good"
            resumptiontoken = None
            fromdate = None
            untildate = None
            idfile = input("idfile: ")
            while idfile == "":
                idfile = input("Bitte geben Sie eine valide ID-Datei an: ")
            while os.path.exists(idfile) == False:
                print("-> Die ID Datei konnte nicht geladen werden")
                idfile = input(
                    f"Bitte geben Sie den Pfad zu einer validen ID-Datei an (relativ zum aktuellen Pfad '{os.getcwd()}' oder absolut): "
                )
            # baseurl und prefix aus der id-Datei auslesen weil die ja jetzt YAML ist und Metadaten enthält
            try:
                # Wir parsen das IDfile später nochmal, daher hier nur die ersten Zeilen (falls das ID File zu groß ist)
                ymlfile = open(idfile, "r", encoding="utf-8")
                lines = ymlfile.read().splitlines()[2:5]
                baseurl =  re.sub(r"\/\s$", "", lines[0].split(": ")[1])
                oaiset = lines[1].split(": ")[1]
                mprefix = lines[2].split(": ")[1]
            except:
                sys.exit("Konnte YAML Datei nicht laden")
            validurl = re.search(URLREGEX, baseurl)
            while validurl is None:
                baseurl = input("Bitte geben Sie eine valide Base-URL ein: ")
                validurl = re.search(URLREGEX, baseurl)
            while mprefix == "":
                mprefix = input("Metadata Prefix: ")
            datengeber = input("Datengeber: ")
            if datengeber == "":
                datengeber = time.strftime("%Y%m%d%H%M%S")
            outputfolder = input("Ordner in den geharvestet werden soll: ")
            if outputfolder == "":
                outputfolder = os.getcwd()
            numberofprocesses = input("Anzahl an parallelen Downloads (default: 16): ")
            if numberofprocesses == "":
                numberofprocesses = 16
            else:
                numberofprocesses = int(numberofprocesses)
        elif howTostart == "R":
            userInput = "good"
            idfile = None
            oaiset = None
            fromdate = None
            untildate = None
            baseurl = input("Base-URL: ")
            validurl = re.search(URLREGEX, baseurl)
            while validurl is None:
                baseurl = input("Bitte geben Sie eine valide Base-URL ein: ")
                validurl = re.search(URLREGEX, baseurl)
            mprefix = input("Metadata Prefix: ")
            while mprefix == "":
                mprefix = input("Metadata Prefix: ")
            resumptiontoken = input("Resumption Token: ")
            while resumptiontoken == "":
                resumptiontoken = input("Resumption Token: ")
            datengeber = input("Datengeber: ")
            if datengeber == "":
                datengeber = time.strftime("%Y%m%d%H%M%S")
            outputfolder = input("Ordner in den geharvestet werden soll: ")
            if outputfolder == "":
                outputfolder = os.getcwd()
            numberofprocesses = input("Anzahl an parallelen Downloads (default: 16): ")
            if numberofprocesses == "":
                numberofprocesses = 16
            else:
                numberofprocesses = int(numberofprocesses)
        elif howTostart == "S":
            userInput = "good"
            baseurl = input("Base-URL: ")
            validurl = re.search(URLREGEX, baseurl)
            while validurl is None:
                baseurl = input("Bitte geben Sie eine valide Base-URL ein: ")
                validurl = re.search(URLREGEX, baseurl)
            getInfos(baseurl, session)
        elif howTostart == "E":
            sys.exit("Programm beendet.")
        else:
            userInput = "bad"
            print(
                "\n-> Die Eingabe wurde nicht verstanden, geben Sie bitte entweder die Großbuchtsaben N, I, S oder R ein oder E zum Beenden des Programms."
            )

    timeout = input("Timeout in Sekunden: ")
    if timeout == "":
        timeout = 0
    else:
        timeout = timeout
    debug = False
    configmode = False

    return (
        baseurl,
        mprefix,
        datengeber,
        oaiset,
        fromdate,
        untildate,
        idfile,
        resumptiontoken,
        float(timeout),
        debug,
        configmode,
        outputfolder,
        numberofprocesses,
    )


def getInfos(baseurl, session):
    # baseurl Parameter löschen, falls fälschlicherweise übergeben
    baseurl = re.sub(r"(\?.+)", "", baseurl)
    try:
        session.get(baseurl, verify=False, timeout=(20, 80))
    except requests.exceptions.HTTPError:
        input(
            f"--------------------------------------\n{Fore.RED}Fehlermeldung:\n  {Style.DIM}Die Schnittstelle ist nicht erreichbar.\n{Style.RESET_ALL}Drücken Sie Enter zum Beenden..."
        )
        if input != "":
            print("Die Schnittstelle ist nicht erreichbar. Breche ab.")
            sys.exit()
    except requests.exceptions.ConnectionError:
        input(
            f"--------------------------------------\n{Fore.RED}Fehlermeldung:\n  {Style.DIM}Die Schnittstelle ist nicht erreichbar.\n{Style.RESET_ALL}Drücken Sie Enter zum Beenden..."
        )
        if input != "":
            sys.exit()
    else:
        pass
    # leeres Dictionary aufmachen
    sets = {}
    prefixes = []

    def getSets(url):

        response = session.get(url, verify=False, timeout=(20, 80))
        try:
            response.headers["Content-Type"]
        except:
            pass
        else:
            if "xml" not in response.headers["Content-Type"]:
                print(
                    f"Abruf der OAI-Sets: Wahrscheinlich kein valides XML im Return der Schnittstelle ({url}) (Content-Type ohne XML)"
                )
        namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        errors = re.findall(r"error\scode=['\"](.+)['\"]>(.*)<\/error", response.text)
        if errors:
            print(f"Fehler: {errors[0][0]} Abbruch.")
            input(
                f"{Fore.RED} Fehler: {Style.DIM} {errors[0][0]} {Style.RESET_ALL} \n--------------------------------------\nDrücken Sie Enter zum Beenden..."
            )
            if input != "":
                sys.exit()
        else:
            try:
                root = etree.XML(response.content)
                lmxlsets = root.findall(f".//oai:set", namespaces)
                for s in lmxlsets:
                    spec = s.findall(f".//oai:setSpec", namespaces)[0].text
                    name = s.findall(f".//oai:setName", namespaces)[0].text
                    if spec and name != None:
                        if name in sets:
                            # print(f"Set '{name}' mehr als einmal auf der Schnittstelle angegeben")
                            pass
                        else:
                            sets[name] = spec
                    else:
                        pass

                # ResumptionTokens

                token = root.findall(f".//oai:resumptionToken", namespaces)
                try:
                    token = token[0].text
                except:
                    pass
                else:
                    # URL encode den resumptionToken (siehe https://gitlab.gwdg.de/maps/harvester/-/issues/25)
                    if token:
                        urllib.parse.quote_plus(token)
                        nexturl = (
                            re.sub(r"&resumptionToken=.+", "", url)
                            + "&resumptionToken="
                            + token
                        )
                        getSets(nexturl)
            except etree.XMLSyntaxError as e:
                print(
                    f"Fehler beim ermitteln der OAI Sets. Syntaxfehler im Return: {e}"
                )
                pass

    def getprefixes(baseurl):

        url = baseurl + "?verb=ListMetadataFormats"
        response = session.get(url, verify=False, timeout=(20, 80))
        try:
            response.headers["Content-Type"]
        except:
            pass
        else:
            if "xml" not in response.headers["Content-Type"]:
                print(
                    f"Abruf der OAI-Metadaten-Prefixe: Wahrscheinlich kein valides XML im Return der Schnittstelle ({url}) (Content-Type ohne XML)"
                )
        for prefix in re.findall(
            r"<metadataPrefix>(.*?)<\/metadataPrefix>", response.text
        ):
            prefixes.append(prefix)
        if len(prefixes) > 0:
            print(
                f"--------------------------------------\nAuf der Schnittstelle {baseurl} sind folgende Metadaten-Prefixe registriert: \n--------------------------------------"
            )
            for i in sorted(prefixes):
                print(i)
        else:
            print(
                f"--------------------------------------\nFür die Schnittstelle {baseurl} konnten keine Metadaten-Prefixe ermittelt werden.\n--------------------------------------"
            )

    getprefixes(baseurl)
    getSets(baseurl + "?verb=ListSets")

    if len(sets) != 0:
        print(
            "--------------------------------------\nAuf der Schnittstelle sind folgende Sets [setspec] vorhanden: \n--------------------------------------"
        )
        for key in sorted(sets.keys(), key=lambda x: x.lower()):
            print(f"{key} [{sets[key]}]")
        input(
            "--------------------------------------\nDrücken Sie Enter zum Beenden..."
        )
        if input != "":
            sys.exit()
    else:
        input(
            "--------------------------------------\nKeine Sets gefunden, ist die URL korrekt? Drücken Sie Enter zum Beenden..."
        )
        if input != "":
            sys.exit()
