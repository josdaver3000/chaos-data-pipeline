"""Corrompe ventas limpias con porcentajes EXACTOS y reproducibles."""

import copy
import random
from datetime import datetime

# Porcentaje de registros afectados por cada tipo de error
DEFAULT_RATES = {
    "nulls": 0.15,        # un campo se vuelve None
    "bad_dates": 0.05,    # fecha en formato incorrecto
    "weird_chars": 0.10,  # texto con caracteres extraños (mojibake)
    "duplicates": 0.05,   # el mismo mensaje llega dos veces
}

# Campos que pueden quedar nulos (event_id y order_date nunca: son clave del pipeline)
NULLABLE_FIELDS = ["customer_name", "email", "city", "unit_price", "quantity"]
TEXT_FIELDS = ["customer_name", "city"]
WEIRD_CHARS = ["Ã±", "Ã©", "Ã¡", "�", "???"]
BAD_DATE_FORMATS = ["%Y/%d/%m", "%d-%m-%Y", "%d/%m/%y", "%Y.%m.%d"]


def _bad_date(value: str, rng: random.Random) -> str:
    """Devuelve la misma fecha en un formato que el pipeline no espera."""
    if rng.random() < 0.2:
        return "ayer"  # ni siquiera es una fecha
    fecha = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return fecha.strftime(rng.choice(BAD_DATE_FORMATS))


def _pick(rng: random.Random, n: int, rate: float) -> list[int]:
    """Índices de exactamente round(n * rate) registros, sin repetir."""
    return rng.sample(range(n), round(n * rate))


def corrupt(records: list[dict], rates: dict | None = None, seed: int = 0) -> tuple[list[dict], dict]:
    """Devuelve (registros corrompidos, reporte). No modifica la lista original."""
    rates = {**DEFAULT_RATES, **(rates or {})}
    rng = random.Random(seed)
    data = copy.deepcopy(records)
    n = len(data)
    report = {"input_records": n, "nulls": 0, "bad_dates": 0, "weird_chars": 0, "duplicates": 0}

    for i in _pick(rng, n, rates["nulls"]):
        data[i][rng.choice(NULLABLE_FIELDS)] = None
        report["nulls"] += 1

    for i in _pick(rng, n, rates["bad_dates"]):
        data[i]["order_date"] = _bad_date(data[i]["order_date"], rng)
        report["bad_dates"] += 1

    for i in _pick(rng, n, rates["weird_chars"]):
        campo = rng.choice(TEXT_FIELDS)
        if isinstance(data[i][campo], str):  # si ya quedó nulo, no hay texto que ensuciar
            data[i][campo] += rng.choice(WEIRD_CHARS)
            report["weird_chars"] += 1

    # Duplicados: copia exacta del mismo mensaje (mismo event_id)
    duplicados = [copy.deepcopy(data[i]) for i in _pick(rng, n, rates["duplicates"])]
    report["duplicates"] = len(duplicados)
    data.extend(duplicados)
    rng.shuffle(data)  # los duplicados no llegan pegados al original

    report["output_records"] = len(data)
    return data, report


if __name__ == "__main__":
    from generate import generate_sales

    limpios = generate_sales(100, seed=42)
    sucios, reporte = corrupt(limpios, seed=42)
    print(reporte)