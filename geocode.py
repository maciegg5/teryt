import pandas as pd
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from multiprocessing import Pool, cpu_count

# ------------------------------------------------------------
# 1. KONFIGURACJA
# ------------------------------------------------------------

 geolocator = Nominatim(
    user_agent="teryt-geocoder (github.com/TWOJ_LOGIN/TWOJE_REPO)",
    timeout=10
)

# Bounding box województwa małopolskiego (Nominatim format):
# (North, West), (South, East)
VIEWBOX = [(50.53, 19.17), (49.18, 21.50)]

def is_in_malopolskie(lat, lon):
    return 49.18 <= lat <= 50.53 and 19.17 <= lon <= 21.50


# ------------------------------------------------------------
# 2. FUNKCJA GEOKODUJĄCA Z RETRY I WYMUSZENIEM VIEWBOX
# ------------------------------------------------------------

def geocode_with_retry(query, max_retries=3):
    for attempt in range(max_retries):
        try:
            location = geolocator.geocode(
                query,
                viewbox=VIEWBOX,
                bounded=True
            )
            return location
        except (GeocoderTimedOut, GeocoderServiceError):
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None
    return None


# ------------------------------------------------------------
# 3. FUNKCJA GEOKODUJĄCA POJEDYNCZY REKORD (NAZWA, GMINA, POWIAT)
# ------------------------------------------------------------

def geocode_row(row):
    nazwa = row['NAZWA']
    nazwa_dod = row['NAZWA_DOD']
    powiat = row.get('POWIAT', "")
    gmina = row.get('GMINA', "")

    queries = [
        f"{nazwa}, {nazwa_dod}, gmina {gmina}, powiat {powiat}, województwo małopolskie, Polska",
        f"{nazwa}, gmina {gmina}, powiat {powiat}, województwo małopolskie, Polska",
        f"{nazwa}, powiat {powiat}, województwo małopolskie, Polska",
        f"{nazwa}, województwo małopolskie, Polska"
    ]

    for query in queries:
        location = geocode_with_retry(query)

        if location:
            lat, lon = location.latitude, location.longitude

            # Dodatkowa walidacja — musi leżeć na terenie woj. małopolskiego
            if is_in_malopolskie(lat, lon):
                return {
                    'latitude': lat,
                    'longitude': lon,
                    'full_address': location.address
                }

        time.sleep(1)  # opóźnienie wymagane przez Nominatim

    return {'latitude': None, 'longitude': None, 'full_address': None}


# ------------------------------------------------------------
# 4. OPAKOWANIE DO MULTIPROCESSING (ważne!)
# ------------------------------------------------------------

def process_row(row_dict):
    return geocode_row(row_dict)


# ------------------------------------------------------------
# 5. GŁÓWNY PROGRAM
# ------------------------------------------------------------

if __name__ == "__main__":

    df = pd.read_excel('terc_urzedowy.xlsx', dtype={
    "WOJ":str,
    "POW":str,
    "GMI":str,
    "RODZ":str
})


    print("Rozpoczynam sekwencyjne geokodowanie (GitHub Actions)...")

    results = []
    for _, row in df.iterrows():
        result = geocode_row(row)
        results.append(result)
        time.sleep(1)  # HARD LIMIT dla Nominatim

    results_df = pd.DataFrame(results)
    df = pd.concat([df.reset_index(drop=True), results_df], axis=1)

    df.to_csv("miejscowosci_z_koordynatami.csv", index=False, encoding="utf-8")

    print("Gotowe.")
