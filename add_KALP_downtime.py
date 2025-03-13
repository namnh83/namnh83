import os
import sys
import re
import csv

from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import NamedTuple, Generator

from LAB import CFG

from LAB.connector import OracleLogin, oracleInterface
from LAB.logger import LAB_LOG as lg


_BASE_DIRECTORY = Path(os.path.dirname(__file__)).parent  # ../gisa-yam-labor
_DATA_DIRECTORY = _BASE_DIRECTORY / "data" / "kalp" / "downtime"
_SQLROOT = _BASE_DIRECTORY / "sql"

assert os.path.isdir(_DATA_DIRECTORY), f"{_DATA_DIRECTORY} is not a valid directory"
assert os.path.isdir(_SQLROOT), f"{_SQLROOT} is not a valid directory"

_INSERT_SQL = "add_KALP_downtime.sql"
_DATE_VARIANTS = [
        "%d.%m.%Y",  # 01.01.1900
        "%Y-%m-%d",  # 1900-01-01
        "%d/%m/%Y",  # 01/01/1900
        "%m/%d/%Y",  # 01/31/1900 (US Format)
        "%B %d, %Y",  # January 01, 1900
        "%d %B %Y",  # 01 January 1900
    ]
_DATE_FORMAT = "%d.%m.%Y"
_DATETIME_V1 = "%d.%m.%Y %H%M"
_DATETIME_V2 = "%d-%m-%Y %H:%M:%S"


class MissingDateError(Exception):
    pass
  

class CsvRow(NamedTuple):
    server_name: str
    date: str
    patch_group: str


@dataclass
class PatchData:
    hostlist: list[str] = field(default_factory=list)
    patchdate: str
    patchgroup: str = ""
    start_date: datetime | None = None
    end_date: datetime | None = None
    data_error: bool = False

    @property
    def patch_details(self) -> str:
        return "\n".join(self.hostlist)


def _parse_patchgroup(patchgroup: str) -> tuple[str, str] | None:
    """
    Parses the patchgroup string and returns start and end times
    Patchgroup value, example: ['1_Mo_2200-0000', '1_Mo_2000-2200', '1_Mo_2200-0100']
    """
    r = re.compile(r"\d{4}-\d{4}")
    match = r.search(patchgroup)
    if not match:
        return None

    return match.group().split("-")


def _parse_patchdate(patchdate: str) -> str | None:
    """Parses the patchdate string and returns a datetime object"""
    for fmt in _DATE_VARIANTS:
        try:
            date_obj = datetime.strptime(patchdate, fmt)
            return date_obj.strftime(_DATE_FORMAT)

        except ValueError:
            continue
    return None


def _calculate_start_and_end_time(date_str: str, start_time_str: str, end_time_str: str) -> tuple[datetime, datetime]:
    """
    Calculates the end time based on start date and end time string
    -> if patchtime ends with 00:00 O'Clock, ist end_time = 23:59 O'clock
    -> in case the end time is in the next day one day muss be added to the gived date
    """
    start_date = datetime.strptime(f"{date_str} {start_time_str}", _DATETIME_V1)

    if end_time_str == "0000":
        end_date = datetime.strptime(f"{date_str} 2359", _DATETIME_V1)
    elif end_time_str < start_time_str:
        end_date = datetime.strptime(f"{date_str} {end_time_str}", _DATETIME_V1) + timedelta(days=1)
    else:
        end_date = datetime.strptime(f"{date_str} {end_time_str}", _DATETIME_V1)
    
    return start_date, end_date


def _parse_date_with_patchgroup(pd: PatchData) -> PatchData:
    """
    For the version that data is provided with date and patchgroup columns
    """
    start_time, end_time = _parse_patchgroup(pd.patchgroup) or (None, None)
    if not start_time:
        pd.data_error = True
        lg.warning(f'Falsches patchgroup-Format | Value "{pd.patchgroup}" ist ungültig.')
        return pd

    date_str = _parse_patchdate(pd.patchdate)
    if not date_str:
        pd.data_error = True
        lg.warning(f'Falsches date-Format | Value "{pd.patchdate}" ist ungültig.')
        return pd

    pd.start_date, pd.end_date = _calculate_start_and_end_time(date_str, start_time, end_time)
    return pd


def _adjust_end_time_for_midnight(end_time: datetime, start_time: datetime) -> datetime:
    """Adjusts the end time to 23:59 if it's midnight"""
    if end_time.time() == datetime.min.time():
        return start_time.replace(hour=23, minute=59)
    
    return end_time


def _parse_nextospatch_date(pd: PatchData) -> PatchData:
    """
    For the version that data is provided with nextospatch column
    In this case end time is start time + 2 hours
    """
    try:
        pd.start_date = datetime.strptime(pd.patchdate, _DATETIME_V2)
    except ValueError:
        try:
             pd.start_date = datetime.strptime(pd.patchdate, _DATETIME_V1)
        except ValueError:
            pd.data_error = True
            lg.warning(f'Falsches Datumsformat | Value "{pd.patchdate}" ist ungültig.')
            return pd

    pd.end_date = pd.start_date + timedelta(hours=2)
    pd.end_date = _adjust_end_time_for_midnight(pd.end_date, pd.start_date)

    return pd


def _get_newest_csv_file(file_dir: os.PathLike) -> list:
    """
    Function to check if a new csv file exits or all files have already been processed
    The newest csv file is csv_files[0]
    """
    csv_files = [file for file in os.listdir(file_dir) if file.endswith(".csv")]
    csv_files.sort(
        key=lambda x: os.path.getctime(os.path.join(file_dir, x)), reverse=True
    )

    if csv_files == []:
        lg.info(f"Keine unbearbeitete csv-Datei gefunden | Verzeichnis: {file_dir}")
        sys.exit(1)

    else:
        return csv_files  # give only the newest csv file back to fetch


def _fetch_csv_file(csv_file: Path) -> Generator[CsvRow, None, None]:
    """Function to read the csv file and with column serverrole"""
    try:
        with open(csv_file, "r", newline="", encoding="ISO-8859-1") as f:
            reader = csv.DictReader(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)

            for row in reader:
                # Convert the keys to lowercase letters
                row_lower = {key.lower(): value for key, value in row.items()}

                if (row_lower["serverrole"].lower() != "database server") or (
                    row_lower["status"].lower() != "production"
                ):
                    continue

                if "date" in row_lower and "patchgroup" in row_lower:
                    yield CsvRow(row_lower["name"], row_lower["date"], row_lower["patchgroup"])
                elif "nextospatch" in row_lower:
                    yield CsvRow(row_lower["name"], row_lower["nextospatch"], "")
                else:
                    raise MissingDateError(
                        "Patchdaten fehlen in der Liste | Spalte 'nextospatch' oder die Kombination von 'date' und 'patchgroup'."
                    )

    except KeyError as key_error:
        lg.error(f"Erforderliche Spalte fehlt in der Liste | Spalte: {key_error}")
        sys.exit(1)

    except Exception as e:
        lg.error(f"Problem bei der Bearbeitung der aktuellen Datei! | Fehler: {str(e)}")
        sys.exit(1)


def _group_servers_by_patch(data: list[CsvRow]) -> dict[tuple[str, str], list[str]]:
    """Groups servers by patchdate and patchgroup"""
    grouped_servers: dict[tuple[str, str], list[str]] = {}
    for server, patchdate, patchgroup in data:
        key = (patchdate, patchgroup)

        if key not in grouped_servers:
            grouped_servers[key] = []

        grouped_servers[key].append(server)

    return grouped_servers


def _get_patch_data_list(csv_file: Path) -> list[PatchData]:
    """Generates patch lists from the readed datas"""
    csv_data = list(_fetch_csv_file(csv_file))
    grouped_servers = _group_servers_by_patch(csv_data)
    patch_data_list: list[PatchData] = []

    for (patchdate, patchgroup), servers in grouped_servers.items():
        patch_data = PatchData(hostlist=servers, patchdate=patchdate, patchgroup=patchgroup)
        patch_data_list.append(patch_data)

    return patch_data_list


def _create_appointment(patchtitel: str, patch_data_list: list[PatchData]) -> bool:
    """Creates appointment per insert in database"""

    # read login datas from the config file
    # ** Aktuell nutzen wir c##samyatama für die Verbindung zu DB
    db_login = OracleLogin(
        user=CFG.kalpLogin["user"],
        password=CFG.kalpLogin["password"],
        service=CFG.kalpLogin["service"],
        host=CFG.kalpLogin["host"],
        port=int(CFG.kalpLogin["port"]),
    )

    rc = True
    # try to connect to the kalp database
    try:
        db_con = oracleInterface(login=db_login, sqlroot=_SQLROOT)

        for patch_data in patch_data_list:
            patch_details = patch_data.patch_details

            if patch_data.patchgroup:
                patch_data = _parse_date_with_patchgroup(patch_data)
            else:
                patch_data = _parse_nextospatch_date(patch_data)

            if patch_data.data_error:
                lg.warning(
                    f'Termin für "{patch_data.patchdate} {patch_data.patchgroup}" kann nicht angelegt werden.\n'
                    f'Betroffenene Server sind: {patch_details}'
                )
                rc = False
                continue

            insert_params = {
                "valid_from": patch_data.start_date,
                "valid_to": patch_data.end_date,
                "description": patchtitel,
                "details": patch_details,
            }

            db_con.execSQL(_INSERT_SQL, **insert_params)
            lg.info(
                f'Downtime-Termin für "{patchtitel}" am {patch_data.start_date} wurde auf KALP angelegt.'
            )

    except ConnectionError as db_error:
        lg.error(f"Fehler bei der Verbindung zur KALP DB | Fehler: {str(db_error)}")
        sys.exit(1)

    except Exception as e:
        lg.error(f"Ein unerwarteter Fehler ist aufgetreten! | Fehler: {str(e)}")
        return False

    return rc


def _rename_csv_file(csv_file: str | os.PathLike):
    """Function to rename file after processing"""
    os.rename(csv_file, f"{csv_file}.bearbeitet")
    lg.info(f'Die Datei "{os.path.basename(csv_file)}" wurde als bearbeitet markiert!')


# main to run the process tasks
def main():  # pragma: no cover
    # list all csv files in the directory
    list_files = _get_newest_csv_file(_DATA_DIRECTORY)
    # read csv file
    for i in range(len(list_files)):
        file_name = list_files[i]  # take the csv file name
        lg.info(f'Die Datei "{file_name}" wird jetzt geprüft und bearbeitet.')

        file_path = _DATA_DIRECTORY / file_name  # take file with complete path
        patchtitel = os.path.splitext(file_name)[
            0
        ]  # take filename without extension as description of patch appointment

        # take the list of affected servers and the dates to create appointment
        patch_data_list = _get_patch_data_list(file_path)

        # insert appointments in db
        result = _create_appointment(patchtitel, patch_data_list)

        # rename file after processing
        _rename_csv_file(file_path)
        
        if not result:
            lg.info(
                f'Die Bearbeitung der Datei "{file_name}" war fehlgeschlagen. Bitte die fehlerhaften Daten prüfen!'
            )


if __name__ == "__main__":  # pragma: no cover
    main()
