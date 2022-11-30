import os, sys
import subprocess
import datetime
import struct
import re
from shutil import copyfile
from shutil import rmtree
from shutil import copytree
from shutil import move
from loguru import logger
from pathlib import Path

timer_start = datetime.datetime.now()

# Aufräumen:
logger.info(
    "Entferne Verzeichnisss 'build', falls aus vorherigem Build-Prozess vorhanden.")
if os.path.isdir("build"):
    rmtree("build")



# Variablen für den Build-Prozess (64 bit Python-Umgebung):
if sys.platform.startswith('linux'):
    logger.info(
        "Build für Linux.")
    distdir = "Linux"
    Path('dist/Linux').mkdir(exist_ok=True)
    subprocess.run(['pyinstaller', '-F',  '--name', 'Ometha', 'start.py'])
    move("dist/Ometha", f"dist/{distdir}/Ometha")
    subprocess.run(['staticx', 'dist/Linux/Ometha', 'dist/Linux/Ometha'])
    copyfile(f"dist/{distdir}/Ometha", "/usr/bin/Ometha")
elif sys.platform.startswith('win32'):
    logger.info(
        "Build für Windows.")
    distdir = "Windows"
    Path('dist/Windows').mkdir(exist_ok=True)
    with open("start.py", "r", encoding="utf-8") as f:
        f = f.read()
        f = re.sub(
            r"({Fore.GREEN}|{Fore.RED}|{Fore.WHITE}|{Fore.YELLOW}|{Style.DIM}|{Style.NORMAL}|{Style.RESET_ALL})", "", f)
        f = re.sub("Fore\.MAGENTA \+ ", "", f)
        f = re.sub(
            r"(import colorama|stream = AnsiToWin32\(sys.stderr\).stream|colorama.init\(autoreset=True\)|from colorama import init, Fore, Back, Style, AnsiToWin32)", "", f)
        open("Ometha_windows.py", "w", encoding="utf-8").write(f)
        subprocess.run(['pyinstaller', '-F',  '--name',
                        'Ometha', 'Ometha_windows.py'], stdout=subprocess.DEVNULL)
        os.remove('Ometha_windows.py')
        move("dist/Ometha.exe", f"dist/{distdir}/Ometha.exe")
elif sys.platform.startswith('darwin'):
    logger.info(
        "Build für macOS.")
    distdir = "macOS"
    Path('dist/macOS').mkdir(exist_ok=True)
    subprocess.run(['pyinstaller', '-F',  '--name', 'Ometha', 'start.py'])
    move("dist/Ometha", f"dist/{distdir}/Ometha")

logger.info(f"Build-Prozess abgeschlossen. Ausgabe im Ordner 'dist/{distdir}'.")

if os.path.isdir("build"):
    rmtree("build")

timer_end = datetime.datetime.now()
processing_duration = timer_end - timer_start
logger.info("Prozessierungsdauer: {processing_duration}", processing_duration=processing_duration)
