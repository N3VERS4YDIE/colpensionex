import csv
import datetime
import os
import shutil
from concurrent.futures import ProcessPoolExecutor

import schedule
from generator import generate_entry_csvs


def approve_solicitude(solicitude: dict) -> bool:
    MEN_RPM_AGE = 57
    WOMEN_RPM_AGE = 62

    try:
        is_in_black_list = is_solicitant_in_black_list(solicitude["documento"])
        caracterization = solicitude["caracterizacion"]
        is_pre_retiree = bool(int(solicitude["es_pre_pensionado"]))
        birth_place = solicitude["lugar_nacimiento"]
        is_man = bool(int(solicitude["es_hombre"]))
        age = int(solicitude["edad"])
        origin_program = solicitude["programa_proveniencia"]
        weeks_in_program = int(solicitude["semanas_programa"])
        public_institution = solicitude["institucion_publica"]
        has_child_in_inpec = bool(int(solicitude["tiene_hijos_inpec"]))
        has_condecoration = bool(int(solicitude["condecoracion"]))
        has_family_in_police = bool(int(solicitude["tiene_familiares_policia"]))
        has_oldest_family_member_in_police = bool(
            int(solicitude["tiene_familiar_policia_mayor_edad"])
        )
        has_disciplinary_observation = bool(
            int(solicitude["tiene_observacion_disciplinaria"])
        )
    except KeyError:
        return False

    if (
        caracterization == "Inhabilitar"
        and move_solicitude_to_black_list(DISABLED_CONTRIBUTORS_FILEPATH, solicitude)
        or caracterization == "Embargar"
        and move_solicitude_to_black_list(SEIZED_CONTRIBUTORS_FILEPATH, solicitude)
        or public_institution in ("Minsalud", "Mininterior")
        and has_disciplinary_observation
        and move_solicitude_to_black_list(BLACK_LIST_FILEPATH, solicitude)
    ):
        return False

    return (
        not is_in_black_list
        and not is_pre_retiree
        and birth_place not in ("Bogotá", "Medellin", "Cali")
        and (
            public_institution == "Armada"
            and has_condecoration
            or public_institution == "Inpec"
            and (has_child_in_inpec or not has_child_in_inpec and has_condecoration)
            or public_institution == "Policia"
            and has_family_in_police
            and has_oldest_family_member_in_police
            or public_institution in ("Minsalud", "Mininterior")
            and not has_disciplinary_observation
        )
        or (
            public_institution == "Armada"
            and not has_condecoration
            or public_institution == "Inpec"
            and not has_child_in_inpec
            and not has_condecoration
            or public_institution == "Policia"
            and has_family_in_police
            and not has_oldest_family_member_in_police
            or not public_institution
            and (is_man and age < MEN_RPM_AGE or not is_man and age < WOMEN_RPM_AGE)
            and any(
                origin_program == program and weeks_in_program > weeks
                for program, weeks in {
                    "Porvenir": 800,
                    "Colfondos": 300,
                    "Old Mutual": 100,
                    "Extranjero": float("-inf"),
                }.items()
            )
        )
    )


def move_solicitude_to_black_list(filepath, solicitude: dict):
    solicitude["fecha_procesamiento"] = datetime.date.today()

    if not os.path.isfile(filepath):
        with open(filepath, mode="w", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(solicitude.keys())

    with open(filepath, mode="a", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(solicitude.values())


def is_solicitant_in_black_list(document: str) -> bool:
    for filepath in (
        DISABLED_CONTRIBUTORS_FILEPATH,
        SEIZED_CONTRIBUTORS_FILEPATH,
        BLACK_LIST_FILEPATH,
    ):
        if os.path.isfile(filepath):
            with open(filepath, mode="r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                rows = list(reader)
                if any(document == row["documento"] for row in rows):
                    return True
    return False


def execute_process(path: str, func: callable):
    with os.scandir(path) as entries:
        with ProcessPoolExecutor() as executor:
            executor.map(func, tuple(entry.path for entry in entries))


def move_solicitude_to_in_process():
    if not os.path.exists(ENTRY_SOLICITUDES_PATH):
        return
    os.makedirs(IN_PROCESS_SOLICITUDES_PATH, exist_ok=True)
    execute_process(ENTRY_SOLICITUDES_PATH, _move_solicitude_to_in_process)


def _move_solicitude_to_in_process(path: str):
    shutil.move(path, IN_PROCESS_SOLICITUDES_PATH)


def process_solitude():
    if not os.path.exists(IN_PROCESS_SOLICITUDES_PATH):
        return

    previous_day = datetime.date.today() - datetime.timedelta(days=1)
    last_day_processed_solicitudes_path = generate_processed_solicitudes_path(
        previous_day
    )

    if os.path.isfile(last_day_processed_solicitudes_path):
        shutil.make_archive(
            last_day_processed_solicitudes_path,
            "zip",
            last_day_processed_solicitudes_path,
        )
        shutil.rmtree(last_day_processed_solicitudes_path)

    os.makedirs(generate_processed_solicitudes_path(), exist_ok=True)

    with os.scandir(IN_PROCESS_SOLICITUDES_PATH) as _solicitude_files:
        solicitude_files = tuple(_solicitude_files)

    with ProcessPoolExecutor() as executor:
        executor.map(
            _process_solicitude,
            tuple(solicitude.path for solicitude in solicitude_files),
        )


def _process_solicitude(path: str):
    with open(path, mode="r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        solicitudes = list(reader)

    for solicitude in solicitudes:
        if approve_solicitude(solicitude):
            solicitude["estado_solicitud"] = "Aprobada"
        elif solicitude["caracterizacion"] == "Inhabilitar":
            solicitude["estado_solicitud"] = "Inhabilitada"
        elif solicitude["caracterizacion"] == "Embargar":
            solicitude["estado_solicitud"] = "Embargada"
        else:
            solicitude["estado_solicitud"] = "Rechazada"

    with open(path, mode="w", encoding="utf-8") as csvfile:
        fieldnames = solicitudes[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(solicitudes)

    shutil.move(path, generate_processed_solicitudes_path())


def generate_processed_solicitudes_path(
    date: datetime.date = datetime.date.today(),
) -> str:
    return f"SolicitudesProcesadas_{date.strftime('%Y_%m_%d')}"


ENTRY_SOLICITUDES_PATH = "SolicitudesEntrantes"
IN_PROCESS_SOLICITUDES_PATH = "SolicitudesEnProcesamiento"

DISABLED_CONTRIBUTORS_FILEPATH = "CotizantesInhabilitados.csv"
SEIZED_CONTRIBUTORS_FILEPATH = "CotizantesEmbargados.csv"
BLACK_LIST_FILEPATH = "ListaNegra.csv"

if __name__ == "__main__":
    schedule.every().hour.at(":00").do(move_solicitude_to_in_process)

    for i in (*range(8, 12), *range(14, 18)):
        schedule.every().day.at(f"{i:02}:15").do(generate_entry_csvs)

    schedule.every().day.at("01:00").do(process_solitude)

    while True:
        schedule.run_pending()

    # generate_entry_csvs()
    # move_solicitude_to_in_process()
    # process_solitude()
