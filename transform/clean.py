"""Limpieza de la capa silver, guiada por un archivo de reglas (schemas/*.yaml).

Son funciones PURAS: reciben registros (dicts) y reglas, y devuelven registros limpios,
registros en cuarentena y un reporte. No tocan S3 ni archivos, por eso se pueden
probar con pytest sin levantar nada.

Principio: limpiar es corregir lo que se puede corregir con certeza. Lo que no se puede
recuperar se deja nulo (no se inventa) o, si es imprescindible, va a cuarentena.
"""
import re
from collections import Counter
from datetime import datetime

import yaml


def load_rules(path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- piezas pequeñas (cada una se prueba por separado) ----------

def to_int(value):
    """Convierte a entero si es posible; si no, None."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"\s*-?\d+\s*", value):
        return int(value)
    return None


def parse_date(value, formats):
    """Prueba cada formato en orden. Devuelve (datetime, formato_usado) o (None, None)."""
    if value is None:
        return None, None
    text = str(value).strip()
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt), fmt
        except ValueError:
            continue
    return None, None


def clean_text(value, noise_patterns, noise=False, lowercase=False):
    """Devuelve (texto limpio o None, si se quitó texto dañado)."""
    if value is None:
        return None, False
    text = str(value)
    had_noise = False
    if noise:
        for pattern in noise_patterns:
            text, n = re.subn(pattern, "", text)
            had_noise = had_noise or n > 0
    text = re.sub(r"\s+", " ", text).strip()
    if lowercase:
        text = text.lower()
    return (text or None), had_noise


def recover_product(rec, result, a, b):
    """Si falta exactamente uno de result, a, b (con result = a * b), lo calcula.

    Devuelve el nombre del campo recuperado, o None si no se pudo.
    """
    values = {result: rec.get(result), a: rec.get(a), b: rec.get(b)}
    missing = [k for k, v in values.items() if v is None]
    if len(missing) != 1:
        return None
    r, x, y = values[result], values[a], values[b]
    if r is None:
        rec[result] = x * y
    elif x is None and y and r % y == 0:
        rec[a] = r // y
    elif y is None and x and r % x == 0:
        rec[b] = r // x
    else:
        return None
    return missing[0]


# ---------- un registro ----------

def clean_record(raw: dict, rules: dict):
    """Devuelve (registro limpio, None) o (None, motivo_de_cuarentena)."""
    noise_patterns = rules.get("noise_patterns", [])
    rec: dict = {}
    flags: list[str] = []

    for name, spec in rules["fields"].items():
        value = raw.get(name)
        kind = spec.get("type", "string")

        if kind == "datetime":
            if value is None:
                return None, f"missing_required:{name}"
            dt, fmt = parse_date(value, spec["formats"])
            if dt is None:
                return None, f"invalid_date:{value}"
            if fmt != spec["formats"][0]:
                flags.append(f"{name}_reformatted")
            if "%H" not in fmt:
                flags.append(f"{name}_time_unknown")
            value = dt.strftime("%Y-%m-%d %H:%M:%S")

        elif kind == "int":
            number = to_int(value)
            if number is not None and number < spec.get("min", number):
                number = None
                flags.append(f"{name}_invalid")
            value = number

        else:  # string
            value, had_noise = clean_text(
                value, noise_patterns, noise=spec.get("noise", False), lowercase=spec.get("lowercase", False)
            )
            if had_noise:
                flags.append(f"{name}_noise_removed")
            if value is not None and "allowed" in spec and value not in spec["allowed"]:
                value = None
                flags.append(f"{name}_invalid")

        if value is None and spec.get("required"):
            return None, f"missing_required:{name}"
        rec[name] = value

    for rule in rules.get("recover", []):
        recovered = recover_product(rec, rule["target"], *rule["of"])
        if recovered:
            flags.append(f"{recovered}_recovered")

    # Si los tres números existen pero no cuadran, no se toca nada: solo se marca.
    for rule in rules.get("recover", []):
        a, b = rule["of"]
        values = (rec.get(rule["target"]), rec.get(a), rec.get(b))
        if None not in values and values[0] != values[1] * values[2]:
            flags.append(f"{rule['target']}_mismatch")

    rec["dq_flags"] = ";".join(flags)
    return rec, None


# ---------- un lote completo ----------

def clean_records(records: list, rules: dict):
    """Devuelve (registros limpios, registros en cuarentena, reporte). No modifica la entrada."""
    key = rules["dedupe_key"]
    quarantine: list[dict] = []
    unique: list[dict] = []
    seen: set = set()
    duplicates = 0

    for raw in records:
        if not isinstance(raw, dict) or raw.get(key) in (None, ""):
            quarantine.append({"reason": "missing_key", "record": raw})
            continue
        if raw[key] in seen:
            duplicates += 1
            continue
        seen.add(raw[key])
        unique.append(raw)

    clean: list[dict] = []
    for raw in unique:
        rec, reason = clean_record(raw, rules)
        if rec is None:
            quarantine.append({"reason": reason, "record": raw})
        else:
            clean.append(rec)

    flag_counts = Counter(f for r in clean for f in r["dq_flags"].split(";") if f)
    report = {
        "rows_in": len(records),
        "duplicates_removed": duplicates,
        "quarantined": len(quarantine),
        "quarantine_reasons": dict(Counter(q["reason"].split(":")[0] for q in quarantine)),
        "rows_out": len(clean),
        "flags": dict(sorted(flag_counts.items())),
        "nulls_remaining": {
            name: sum(1 for r in clean if r[name] is None) for name in rules["fields"]
        },
    }
    return clean, quarantine, report
