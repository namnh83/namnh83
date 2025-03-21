import openpyxl
import random
import webbrowser
import time
import pyautogui
import urllib.parse
import pygetwindow as gw

_suchmaschinen = [
    "https://duckduckgo.com/?q={}",
    "https://www.bing.com/search?q={}",
    "https://duckduckgo.com/?q={}&t=h_&ia=web",
]
    # "https://www.ecosia.org/search?q={}",
    
def google_suche_aus_excel(excel_datei, spalte, start_zeile):
    try:
        workbook = openpyxl.load_workbook(excel_datei)
        sheet = workbook.active

        zeile = start_zeile
        while sheet[f"{spalte}{zeile}"].value is not None:
            suchbegriff = f"{str(sheet[f"{spalte}{zeile}"].value)} geschaeftsfuehrer"

            suchmaschine_url = random.choice(_suchmaschinen)
            suchbegriff_url = urllib.parse.quote_plus(suchbegriff)

            google_url = suchmaschine_url.format(suchbegriff_url)
            # Browserfenster finden und in den Vordergrund bringen
            browser_window = gw.getWindowsWithTitle("Google Chrome")[0]

            if browser_window:
                browser_window.activate()
                time.sleep(0.5)  # Kurze Pause, um sicherzustellen, dass das Fenster aktiv ist

                # Aktuellen Tab schließen (betriebssystemabhängig)
                pyautogui.FAILSAFE = False
                pyautogui.hotkey('ctrl', 'w')  # Windows/Linux
                # pyautogui.hotkey('command', 'w') # MacOS
                time.sleep(1) # kurze Pause damit der Tab auch geschlossen wird.

                # Neue Suche im selben Fenster öffnen
                webbrowser.open(google_url)
                print(f"Suche nach '{suchbegriff}' durchgeführt. Bitte Ergebnisse überprüfen.")

                time.sleep(12)  # 12 Sekunden warten

            zeile += 1

    except FileNotFoundError:
        print(f"Fehler: Die Datei '{excel_datei}' wurde nicht gefunden.")
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")


def main():
    excel_datei = "D:\GoogleSearch\lead_search.xlsx"  # Ersetzen Sie dies durch Ihren Dateinamen
    spalte = "A"
    start_zeile = 2
    google_suche_aus_excel(excel_datei, spalte, start_zeile)


if __name__ == "__main__":
    main()
