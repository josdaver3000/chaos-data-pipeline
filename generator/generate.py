"""Genera ventas de e-commerce LIMPIAS y reproducibles (misma semilla = mismos datos)."""
import random
import uuid

from faker import Faker

CIUDADES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Cúcuta", "Pereira", "Bucaramanga"]

# (producto, categoría, precio mínimo, precio máximo) en COP
PRODUCTOS = [
    ("Camiseta oversize", "ropa", 35_000, 90_000),
    ("Hoodie", "ropa", 80_000, 180_000),
    ("Tenis urbanos", "calzado", 150_000, 380_000),
    ("Gorra", "accesorios", 25_000, 70_000),
    ("Audífonos bluetooth", "tecnologia", 60_000, 250_000),
    ("Termo 1L", "hogar", 30_000, 85_000),
]

METODOS_PAGO = ["tarjeta", "pse", "efectivo", "nequi"]
ESTADOS = ["pagado", "enviado", "entregado", "cancelado"]


def generate_sale(fake: Faker, rng: random.Random) -> dict:
    """Una venta limpia. Todo el azar sale de fake/rng, que están sembrados."""
    producto, categoria, precio_min, precio_max = rng.choice(PRODUCTOS)
    cantidad = rng.randint(1, 5)
    precio = int(round(rng.randint(precio_min, precio_max), -2))
    fecha = fake.date_time_between(start_date="-30d", end_date="now")

    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "order_date": fecha.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_name": fake.name(),
        "email": fake.email(),
        "city": rng.choice(CIUDADES),
        "product": producto,
        "category": categoria,
        "quantity": cantidad,
        "unit_price": precio,
        "total": cantidad * precio,
        "payment_method": rng.choice(METODOS_PAGO),
        "status": rng.choice(ESTADOS),
    }


def generate_sales(n: int, seed: int) -> list[dict]:
    """n ventas limpias. Con la misma semilla siempre salen las mismas."""
    fake = Faker("es_CO")
    fake.seed_instance(seed)
    rng = random.Random(seed)
    return [generate_sale(fake, rng) for _ in range(n)]


if __name__ == "__main__":
    for venta in generate_sales(3, seed=42):
        print(venta)