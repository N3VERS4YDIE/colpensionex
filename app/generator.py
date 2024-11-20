import csv
import os
import random
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

from faker import Faker


def generate_entry_csvs():
    os.makedirs(CARACTERIZATIONS_PATH, exist_ok=True)
    os.makedirs(SOLICITUDES_PATH, exist_ok=True)

    with ProcessPoolExecutor() as executor:
        executor.map(_generate_entry_csvs, range(FILES))


def _generate_entry_csvs(index: int):
    """Generates a single CSV file with 100 rows."""
    ROWS = 100
    caracterizations_filename = (
        f"{CARACTERIZATIONS_PATH}/Caracterizaciones {datetime.now()} ({index+1}).csv"
    )
    solicitude_filename = (
        f"{SOLICITUDES_PATH}/Solicitudes {datetime.now()} ({index+1}).csv"
    )

    caracterizations = tuple(generate_caracterization() for _ in range(ROWS + 1))
    solicitudes = tuple(
        {"documento": caracterization["documento"], **generate_solicitude()}
        for caracterization in caracterizations
    )

    with open(caracterizations_filename, "w", encoding="utf-8") as f1:
        writer = csv.DictWriter(f1, fieldnames=[*caracterizations[0].keys()])
        writer.writeheader()
        writer.writerows(caracterizations)

    with open(solicitude_filename, "w", encoding="utf-8") as f2:
        writer2 = csv.DictWriter(f2, fieldnames=[*solicitudes[0].keys()])
        writer2.writeheader()
        writer2.writerows(solicitudes)


def generate_caracterization() -> dict:
    """Generates a single row for the caracterization CSV file."""
    return {
        "documento": random.randint(1_000_000_000, 9_999_999_999),
        "nombre_completo": fake.name(),
        "caracterizacion": random.choice(["Inhailitar", "Embargar"]),
    }


def generate_solicitude() -> dict:
    """Generates a single row for the solicitude CSV file."""

    return {
        "estado_solicitud": "Generada",
        "es_pre_pensionado": random.randint(0, 1),
        "lugar_nacimiento": random.choice(cities),
        "lugar_residencia": random.choice(cities),
        "es_hombre": random.randint(0, 1),
        "edad": random.randint(14, 70),
        "programa_proveniencia": random.choice(
            ["Porvenir", "Colfondos", "Old Mutual", "Extranjero"]
        ),
        "semanas_programa": random.randint(50, 1200),
        "institucion_publica": random.choice(
            ["Armada", "Ejercito", "Policia", "Minsalud", "Mininterior", "Inpec"]
        ),
        "tiene_hijos_inpec": random.randint(0, 1),
        "condecoracion": random.randint(0, 1),
        "tiene_familiares_policia": random.randint(0, 1),
        "tiene_familiar_policia_mayor_edad": random.randint(0, 1),
        "tiene_observacion_disciplinaria": random.randint(0, 1),
    }


FILES = 10
CARACTERIZATIONS_PATH = "CaracterizacionesEntrantes"
SOLICITUDES_PATH = "SolicitudesEntrantes"

fake = Faker()

with open("ciudades.csv", encoding="utf-8") as f:
    cities = tuple(
        f"{row['Ciudad']} - {row['Departamento']}" for row in csv.DictReader(f)
    )


if __name__ == "__main__":
    generate_entry_csvs()
