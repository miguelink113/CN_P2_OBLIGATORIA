import json
import hashlib

TEAMS = [
    "Servigroup Poniente Benidorm",
    "Isla Maxorata",
    "Beachbol Valencia",
    "VCP Barcelona",
    "CVP NET 7 Gran Canaria",
    "Volei Praia Vigo",
    "Switch Volley",
    "VP Madrid",
    "NZA Beach Volley Academia",
    "CV Las Rozas",
]

INPUT_FILE = "ranking_nacional_voleyplaya.json"
OUTPUT_FILE = "ranking_nacional_voleyplaya_con_equipos_random.json"


def assign_team_deterministic(id_persona: str) -> str:
    """
    Asigna un equipo de forma determinista a partir del IdPersona
    """
    hash_value = hashlib.sha256(id_persona.encode("utf-8")).hexdigest()
    index = int(hash_value, 16) % len(TEAMS)
    return TEAMS[index]


def main():
    # Cargar jugadores
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        jugadores = json.load(f)

    jugadores_modificados = []

    for jugador in jugadores:
        # Eliminar campo PuntosSinFormato
        jugador.pop("PuntosSinFormato", None)

        # Asignar equipo determinista
        jugador["EquipoVoleyPlaya"] = assign_team_deterministic(jugador["IdPersona"])

        jugadores_modificados.append(jugador)

    # Guardar resultado
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(jugadores_modificados, f, ensure_ascii=False, indent=2)

    print(f"Archivo generado correctamente: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
