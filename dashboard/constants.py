# This file stores constants used across the dashboard app, such as file paths.


from pathlib import Path

DATA_PATH = Path("../data/gdelt_project.gold.suri_20260226.csv")

# Minimal CAMEO / ISO-style mapping (extend as needed)
COUNTRY_MAP = {
    "USA": "United States",
    "CHN": "China",
    "RUS": "Russia",
    "GBR": "United Kingdom",
    "FRA": "France",
    "DEU": "Germany",
    "UKR": "Ukraine",
    "ISR": "Israel",
    "PSE": "Palestine",
    "IRN": "Iran",
    "SYR": "Syria",
    "IND": "India",
    "BRA": "Brazil",
    "MEX": "Mexico",
    "TUR": "Turkey",
    "COD": "Democratic Republic of the Congo",
    "RWA": "Rwanda",
    "ETH": "Ethiopia",
    "ERI": "Eritrea",
    "PAK": "Pakistan",
    "EGY": "Egypt",
    "VEN": "Venezuela",
    "ARG": "Argentina",
    "PRK": "North Korea",
    "TWN": "Taiwan",
    "KOR": "South Korea"
}