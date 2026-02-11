#!/usr/bin/env python3
"""Auswertung der Nutzungszeiten FFK.

Liest Buchungsdaten (Excel oder CSV), filtert nach definierten Kriterien
und wertet die Nutzungsdauer nach Veranstalter-Kategorien und Räumen aus.
"""

from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"

# Eingabedateien: (Dateiname, Jahres-Label)
INPUT_FILES = [
    ("VA_Buchungen_evis_2024.csv", "2024"),
    ("VA_Buchungen_evis_2025.xlsx", "2025"),
]

# Spaltennamen (kanonisch, nach Normalisierung)
COL_STATUS = "Buchungsstatus"
COL_ROOM = "VA_Raum0"
COL_BOOKING_NAME = "VA_Buchung_Name"
COL_VERANSTALTER = "Veranstalter_1"
COL_DAUER = "Dauer"

# Filterwerte
REQUIRED_STATUS = "Vtg ok"

EXCLUDED_ROOMS = {
    "Salon Godet",
    "Rotunde",
    "Restaurant Hugo & Notte",
}

EXCLUDED_NAME_KEYWORDS = [
    "Umbauten",
    "Betriebsferien",
    "Catering",
    "Flügelnutzung",
    "grobe Reservierung",
    "Nutzung",
    "Technik",
]

# Veranstalter-Kategorien
CAT1_LABEL = "Evangelische Akademie zu Berlin gGmbH"
CAT2_LABEL = (
    "Bevollmächtigte des Rates der EKD bei der Bundesrepublik Deutschland "
    "und der Europäischen Union"
)
CAT3_LABEL = "Externe Veranstalter (alle übrigen)"


# ---------------------------------------------------------------------------
# Daten laden und bereinigen
# ---------------------------------------------------------------------------

def load_data(filepath: Path) -> pd.DataFrame:
    suffix = filepath.suffix.lower()
    if suffix == ".xlsx":
        df = pd.read_excel(filepath, engine="openpyxl")
    elif suffix == ".csv":
        df = pd.read_csv(filepath, sep=";", encoding="latin-1")
    else:
        raise ValueError(f"Unbekanntes Dateiformat: {suffix}")

    # Spalte VA_Raum → VA_Raum0 normalisieren (2024-CSV hat nur VA_Raum)
    if "VA_Raum" in df.columns and "VA_Raum0" not in df.columns:
        df = df.rename(columns={"VA_Raum": "VA_Raum0"})

    # Whitespace in String-Spalten entfernen
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # Dauer als numerischen Wert sicherstellen
    df[COL_DAUER] = pd.to_numeric(df[COL_DAUER], errors="coerce").fillna(0.0)

    return df


# ---------------------------------------------------------------------------
# Filter anwenden
# ---------------------------------------------------------------------------

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    # 1. Buchungsstatus == "Vtg ok"
    mask_status = df[COL_STATUS] == REQUIRED_STATUS

    # 2. Bestimmte Räume ausschließen
    mask_room = ~df[COL_ROOM].isin(EXCLUDED_ROOMS)

    # 3. Buchungsnamen mit bestimmten Schlüsselwörtern ausschließen
    pattern = "|".join(EXCLUDED_NAME_KEYWORDS)
    mask_name = ~df[COL_BOOKING_NAME].str.contains(pattern, case=False, na=False)

    return df[mask_status & mask_room & mask_name].copy()


# ---------------------------------------------------------------------------
# Kategorisierung und Aggregation
# ---------------------------------------------------------------------------

def categorize_and_aggregate(df: pd.DataFrame) -> dict:
    def sum_by_room(subset: pd.DataFrame) -> pd.DataFrame:
        if subset.empty:
            return pd.DataFrame(columns=[COL_ROOM, COL_DAUER])
        result = (
            subset.groupby(COL_ROOM, as_index=False)[COL_DAUER]
            .sum()
            .sort_values(COL_ROOM)
        )
        total = pd.DataFrame({
            COL_ROOM: ["GESAMT"],
            COL_DAUER: [result[COL_DAUER].sum()],
        })
        return pd.concat([result, total], ignore_index=True)

    cat1 = df[df[COL_VERANSTALTER] == CAT1_LABEL]
    cat2 = df[df[COL_VERANSTALTER] == CAT2_LABEL]
    cat3 = df[~df[COL_VERANSTALTER].isin([CAT1_LABEL, CAT2_LABEL])]

    return {
        CAT1_LABEL: sum_by_room(cat1),
        CAT2_LABEL: sum_by_room(cat2),
        CAT3_LABEL: sum_by_room(cat3),
    }


# ---------------------------------------------------------------------------
# Ausgabe: Konsole
# ---------------------------------------------------------------------------

def print_results(categories: dict, year: str) -> None:
    print("=" * 70)
    print(f"AUSWERTUNG NUTZUNGSZEITEN FFK {year}")
    print("=" * 70)

    for label, table in categories.items():
        print(f"\n{'─' * 70}")
        print(f"Veranstalter: {label}")
        print(f"{'─' * 70}")
        print(f"{'Raum':<40} {'Dauer (Std)':>12}")
        print(f"{'─' * 40} {'─' * 12}")

        for _, row in table.iterrows():
            name = row[COL_ROOM]
            hours = row[COL_DAUER]
            if name == "GESAMT":
                print(f"{'─' * 40} {'─' * 12}")
            print(f"{name:<40} {hours:>12.2f}")
    print()


# ---------------------------------------------------------------------------
# Ausgabe: Markdown
# ---------------------------------------------------------------------------

def write_markdown(categories: dict, outpath: Path, year: str,
                   source_name: str) -> None:
    lines = [
        f"# Auswertung Nutzungszeiten FFK {year}\n",
        f"Quelle: `{source_name}`\n",
        "## Filterkriterien\n",
        f"- Buchungsstatus = \"{REQUIRED_STATUS}\"",
        f"- Ausgeschlossene Räume: {', '.join(sorted(EXCLUDED_ROOMS))}",
        f"- Ausgeschlossene Buchungsnamen (Schlüsselwörter): "
        f"{', '.join(EXCLUDED_NAME_KEYWORDS)}",
        "",
    ]

    for label, table in categories.items():
        lines.append(f"## {label}\n")
        lines.append("| Raum | Dauer (Std) |")
        lines.append("|:-----|------------:|")
        for _, row in table.iterrows():
            room = row[COL_ROOM]
            hours = row[COL_DAUER]
            bold = "**" if room == "GESAMT" else ""
            lines.append(f"| {bold}{room}{bold} | {bold}{hours:.2f}{bold} |")
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
    combined.columns = ["Kategorie", "Raum", "Dauer_Stunden"]
    combined["Dauer_Stunden"] = combined["Dauer_Stunden"].round(2)

    outpath.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(outpath, index=False, encoding="utf-8-sig")
    print(f"CSV geschrieben: {outpath}")


# ---------------------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------------------

def main():
    for filename, year in INPUT_FILES:
        filepath = BASE_DIR / filename
        if not filepath.exists():
            print(f"WARNUNG: {filepath} nicht gefunden, überspringe.")
            continue

        print(f"\nLade Daten aus: {filepath.name}")
        df = load_data(filepath)
        print(f"  Geladene Zeilen: {len(df)}")

        filtered = apply_filters(df)
        print(f"  Zeilen nach Filterung: {len(filtered)}")

        categories = categorize_and_aggregate(filtered)
        print_results(categories, year)

        md_path = OUTPUT_DIR / f"nutzungszeiten_{year}.md"
        csv_path = OUTPUT_DIR / f"nutzungszeiten_{year}.csv"
        write_markdown(categories, md_path, year, filepath.name)
        write_csv(categories, csv_path)


if __name__ == "__main__":
    main()
