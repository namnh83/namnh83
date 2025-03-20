import webbrowser
import openpyxl
import time
import pyautogui

def google_suche_aus_excel(excel_datei, spalte='A', start_zeile=1):
    """
    Öffnet Google im Browser, liest Suchbegriffe aus einer Excel-Datei,
    gibt sie in das Suchfeld ein und wartet 15 Sekunden.

    Args:
        excel_datei (str): Pfad zur Excel-Datei.
        spalte (str): Spalte, die die Suchbegriffe enthält (z.B. 'A').
        start_zeile (int): Zeile, ab der die Suchbegriffe gelesen werden sollen.
    """
    try:
        # Excel-Datei öffnen
        workbook = openpyxl.load_workbook(excel_datei)
        sheet = workbook.active

        # Google im Standardbrowser öffnen
        webbrowser.open_new_tab("https://www.google.com")
        time.sleep(2)  # Zeit zum Laden der Seite geben

        # Suchbegriffe aus Excel lesen und suchen
        zeile = start_zeile
        while sheet[f"{spalte}{zeile}"].value is not None:
            suchbegriff = str(sheet[f"{spalte}{zeile}"].value)
            pyautogui.write(suchbegriff)
            pyautogui.press("enter")
            print(f"Suche nach '{suchbegriff}' durchgeführt. Bitte Ergebnisse überprüfen.")

            time.sleep(15)  # 15 Sekunden warten

            # Neues Tab für die nächste Suche öffnen
            pyautogui.hotkey('ctrl', 't')
            pyautogui.write("https://www.google.com")
            pyautogui.press("enter")
            time.sleep(2)

            zeile += 1

    except FileNotFoundError:
        print(f"Fehler: Die Datei '{excel_datei}' wurde nicht gefunden.")
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")

# Beispielaufruf
excel_datei = "suchbegriffe.xlsx"  # Ersetzen Sie dies durch Ihren Dateinamen
google_suche_aus_excel(excel_datei)
