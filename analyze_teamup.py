#!/usr/bin/env python3
"""Auswertung der TeamUp-Nutzungszeiten FFK.

Liest TeamUp-Kalender-Rohdaten (CSV), filtert nach Calendar Name und Subject,
berechnet Nutzungszeiten und wertet nach Veranstaltungskategorie und Raum aus.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

# Eingabedateien: (Dateiname, Jahres-Label, Encoding)
INPUT_FILES = [
    ("teamup_2024-raw.xlsx.csv", "2024", "latin-1"),
    ("team-up_2025-raw.csv", "2025", "utf-8-sig"),
]

# Relevante Spalten
COL_SUBJECT = "Subject"
COL_START_DATE = "Start Date"
COL_START_TIME = "Start Time"
COL_END_DATE = "End Date"
COL_END_TIME = "End Time"
COL_CALENDAR = "Calendar Name"
COL_WO = "Wo"
COL_DAUER = "Dauer_Std"

# Calendar Names einschließen
INCLUDED_CALENDARS = {
    "EAzB > EAzB - Festbuchung": "EAzB",
    "Kirche > Frz. Kirche - Festbuchung": "Frz. Kirche",
    "Kirche > Kirchenmusik": "Kirchenmusik",
}

# Subject-Schlüsselwörter ausschließen
EXCLUDED_SUBJECT_KEYWORDS = [
    "Aufbau",
    "Besichtigung",
    "Besichtigungstermin",
    "Flügel",
    "Rückbau",
    "Orgelstimmung",
]

# Zielräume
TARGET_ROOMS = ["Kirchensaal", "Georges-Casalis-Saal", "Entrée"]

# Subject-Kategorien
CAT_OFFENE_KIRCHE = "Offene Kirche"
CAT_ORGELPROBE = "Orgelprobe"
CAT_REST = "Alle verbleibenden Einträge"


# ---------------------------------------------------------------------------
# Daten laden und bereinigen
# ---------------------------------------------------------------------------

def load_data(filepath: Path, encoding: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, sep=";", encoding=encoding)

    # Whitespace in String-Spalten entfernen
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # Leere/ungültige Zeilen entfernen (Subject leer oder "nan")
    df = df[
        (df[COL_SUBJECT] != "") &
        (df[COL_SUBJECT] != "nan") &
        (df[COL_START_DATE] != "") &
        (df[COL_START_DATE] != "nan")
    ].copy()

    return df


# ---------------------------------------------------------------------------
# Dauerberechnung
# ---------------------------------------------------------------------------

def calculate_duration(df: pd.DataFrame) -> pd.DataFrame:
    start = pd.to_datetime(
        df[COL_START_DATE] + " " + df[COL_START_TIME],
        format="%d.%m.%Y %H:%M",
        errors="coerce",
    )
    end = pd.to_datetime(
        df[COL_END_DATE] + " " + df[COL_END_TIME],
        format="%d.%m.%Y %H:%M",
        errors="coerce",
    )
    df[COL_DAUER] = (end - start).dt.total_seconds() / 3600

    # Ungültige Dauern behandeln
    invalid = df[COL_DAUER].isna() | (df[COL_DAUER] <= 0)
    if invalid.any():
        n = invalid.sum()
        print(f"  WARNUNG: {n} Einträge mit ungültiger Dauer (entfernt)")
        df = df[~invalid].copy()

    return df


# ---------------------------------------------------------------------------
# Filter anwenden
# ---------------------------------------------------------------------------

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Nur erlaubte Calendar Names
    mask_calendar = df[COL_CALENDAR].isin(INCLUDED_CALENDARS.keys())

    # 2. Subject-Ausschlüsse
    pattern = "|".join(EXCLUDED_SUBJECT_KEYWORDS)
    mask_subject = ~df[COL_SUBJECT].str.contains(pattern, case=False, na=False)

    return df[mask_calendar & mask_subject].copy()


# ---------------------------------------------------------------------------
# Räume aufsplitten und filtern
# ---------------------------------------------------------------------------

def explode_rooms(df: pd.DataFrame) -> pd.DataFrame:
    # Wo-Spalte nach Komma splitten
    df[COL_WO] = df[COL_WO].str.split(",")
    df = df.explode(COL_WO)
    df[COL_WO] = df[COL_WO].str.strip()

    # Nur Zielräume behalten
    df = df[df[COL_WO].isin(TARGET_ROOMS)].copy()

    return df


# ---------------------------------------------------------------------------
# Kategorisierung und Aggregation
# ---------------------------------------------------------------------------

def categorize_and_aggregate(df: pd.DataFrame) -> dict:
    # Calendar Name auf Kurzlabel mappen
    df["Kalender"] = df[COL_CALENDAR].map(INCLUDED_CALENDARS)

    # Subject-Kategorien
    mask_offene = df[COL_SUBJECT] == CAT_OFFENE_KIRCHE
    mask_orgel = df[COL_SUBJECT].str.contains("Orgelprobe", case=False, na=False)

    cat_offene = df[mask_offene]
    cat_orgel = df[mask_orgel & ~mask_offene]
    cat_rest = df[~mask_offene & ~mask_orgel]

    def pivot_by_room(subset: pd.DataFrame) -> pd.DataFrame:
        if subset.empty:
            result = pd.DataFrame(
                columns=["Kalender"] + TARGET_ROOMS + ["Gesamt"]
            )
            return result

        pivot = (
            subset.groupby(["Kalender", COL_WO])[COL_DAUER]
            .sum()
            .unstack(fill_value=0)
        )

        # Fehlende Raumspalten ergänzen
        for room in TARGET_ROOMS:
            if room not in pivot.columns:
                pivot[room] = 0.0

        pivot = pivot[TARGET_ROOMS]
        pivot["Gesamt"] = pivot.sum(axis=1)
        pivot = pivot.reset_index().sort_values("Kalender")

        # Gesamtzeile
        total = pd.DataFrame([{
            "Kalender": "GESAMT",
            **{room: pivot[room].sum() for room in TARGET_ROOMS},
            "Gesamt": pivot["Gesamt"].sum(),
        }])
        return pd.concat([pivot, total], ignore_index=True)

    return {
        CAT_OFFENE_KIRCHE: pivot_by_room(cat_offene),
        CAT_ORGELPROBE: pivot_by_room(cat_orgel),
        CAT_REST: pivot_by_room(cat_rest),
    }


# ---------------------------------------------------------------------------
# Ausgabe: Konsole
# ---------------------------------------------------------------------------

def print_results(categories: dict, year: str) -> None:
    print("=" * 80)
    print(f"AUSWERTUNG TEAMUP-NUTZUNGSZEITEN FFK {year}")
    print("=" * 80)

    for label, table in categories.items():
        print(f"\n{'─' * 80}")
        print(f"Kategorie: {label}")
        print(f"{'─' * 80}")
        print(
            f"{'Kalender':<20} "
            f"{'Kirchensaal':>14} "
            f"{'G.-Casalis-S.':>14} "
            f"{'Entrée':>14} "
            f"{'Gesamt':>10}"
        )
        print(f"{'─' * 20} {'─' * 14} {'─' * 14} {'─' * 14} {'─' * 10}")

        for _, row in table.iterrows():
            name = row["Kalender"]
            if name == "GESAMT":
                print(
                    f"{'─' * 20} {'─' * 14} {'─' * 14} {'─' * 14} {'─' * 10}"
                )
            print(
                f"{name:<20} "
                f"{row['Kirchensaal']:>14.2f} "
                f"{row['Georges-Casalis-Saal']:>14.2f} "
                f"{row['Entrée']:>14.2f} "
                f"{row['Gesamt']:>10.2f}"
            )
    print()


# ---------------------------------------------------------------------------
# Ausgabe: Markdown
# ---------------------------------------------------------------------------

def write_markdown(categories: dict, outpath: Path, year: str,
                   source_name: str) -> None:
    lines = [
        f"# Auswertung TeamUp-Nutzungszeiten FFK {year}\n",
        f"Quelle: `{source_name}`\n",
        "## Filterkriterien\n",
        f"- Calendar Names: {', '.join(INCLUDED_CALENDARS.values())}",
        f"- Ausgeschlossene Subjects (Schlüsselwörter): "
        f"{', '.join(EXCLUDED_SUBJECT_KEYWORDS)}",
        f"- Räume: {', '.join(TARGET_ROOMS)}",
        "",
    ]

    for label, table in categories.items():
        lines.append(f"## {label}\n")
        lines.append(
            "| Kalender | Kirchensaal | Georges-Casalis-Saal | Entrée | Gesamt |"
        )
        lines.append("|:---------|------------:|---------------------:|-------:|-------:|")
        for _, row in table.iterrows():
            name = row["Kalender"]
            bold = "**" if name == "GESAMT" else ""
            lines.append(
                f"| {bold}{name}{bold} "
                f"| {bold}{row['Kirchensaal']:.2f}{bold} "
                f"| {bold}{row['Georges-Casalis-Saal']:.2f}{bold} "
                f"| {bold}{row['Entrée']:.2f}{bold} "
                f"| {bold}{row['Gesamt']:.2f}{bold} |"
            )
        lines.append("")

    outpath.parent.mkdir(parents=True, exist_ok=True)
    outpath.write_text("\n".join(lines), encoding="utf-8")
    print(f"Markdown geschrieben: {outpath}")


# ---------------------------------------------------------------------------
# Ausgabe: CSV
# ---------------------------------------------------------------------------

def write_csv(categories: dict, outpath: Path) -> None:
    frames = []
    for label, table in categories.items():
        t = table.copy()
        t.insert(0, "Kategorie", label)
        frames.append(t)

    combined = pd.concat(frames, ignore_index=True)
    for col in TARGET_ROOMS + ["Gesamt"]:
        combined[col] = combined[col].round(2)

    outpath.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(outpath, index=False, encoding="utf-8-sig")
    print(f"CSV geschrieben: {outpath}")


# ---------------------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------------------

def main():
    for filename, year, encoding in INPUT_FILES:
        filepath = BASE_DIR / filename
        if not filepath.exists():
            print(f"WARNUNG: {filepath} nicht gefunden, überspringe.")
            continue

        print(f"\nLade Daten aus: {filepath.name}")
        df = load_data(filepath, encoding)
        print(f"  Geladene Zeilen (nach Bereinigung): {len(df)}")

        df = calculate_duration(df)
        print(f"  Zeilen mit gültiger Dauer: {len(df)}")

        filtered = apply_filters(df)
        print(f"  Zeilen nach Filterung: {len(filtered)}")

        exploded = explode_rooms(filtered)
        print(f"  Einträge nach Raum-Aufsplittung (Zielräume): {len(exploded)}")

        categories = categorize_and_aggregate(exploded)
        print_results(categories, year)

        md_path = OUTPUT_DIR / f"teamup_nutzungszeiten_{year}.md"
        csv_path = OUTPUT_DIR / f"teamup_nutzungszeiten_{year}.csv"
        write_markdown(categories, md_path, year, filepath.name)
        write_csv(categories, csv_path)


if __name__ == "__main__":
    main()
