# Planeorama Setup Guide

## What this does
Fetches daily departure data from the OpenSky Network API for a list of airports,
resolves aircraft type and carrier name, and saves the results as per-airport CSVs
in Google Drive. Runs automatically every day via GitHub Actions.

---

## One-time setup

### 1. OpenSky API credentials

1. Log in at https://opensky-network.org
2. Go to **My OpenSky → API Clients**
3. Create a new client — note down the **Client ID** and **Client Secret**

---

### 2. Google Cloud service account

You need a service account so GitHub Actions can write to your Drive without
a browser login.

1. Go to https://console.cloud.google.com
2. Create a new project (or select an existing one) — any name is fine
3. In the search bar, search for **"Drive API"** and click **Enable**
4. Go to **IAM & Admin → Service Accounts → Create Service Account**
   - Name: `planeorama` (or anything you like)
   - Click through the remaining steps (no extra roles needed)
5. Click the service account you just created → **Keys → Add Key → JSON**
   - A JSON file will download — keep it safe, you only get it once
6. Open your Google Drive folder (the one you want CSVs saved to)
   - Click **Share** and add the service account's email address as an **Editor**
   - The email looks like `planeorama@your-project.iam.gserviceaccount.com`
7. Copy the **folder ID** from the URL:
   `https://drive.google.com/drive/folders/`**`THIS_PART_IS_THE_ID`**

---

### 3. GitHub repository secrets

In your GitHub repo go to **Settings → Secrets and variables → Actions → New repository secret**.

Add these four secrets:

| Secret name                  | Value                                              |
|------------------------------|----------------------------------------------------|
| `OPENSKY_CLIENT_ID`          | From step 1                                        |
| `OPENSKY_CLIENT_SECRET`      | From step 1                                        |
| `GOOGLE_SERVICE_ACCOUNT_JSON`| The **entire contents** of the JSON key file       |
| `GOOGLE_DRIVE_FOLDER_ID`     | The folder ID from step 2                          |

---

### 4. First run

Trigger a manual run from **Actions → Daily Flight Data Collection → Run workflow**.

On the first run the script will:
- Download the OpenFlights airline database and save it to Drive
- Fetch yesterday's departures for all airports
- Begin backfilling toward `BACKFILL_TO_DATE` (default: 2024-01-01) with remaining credits

Subsequent daily runs will forward-fill yesterday's data first, then continue backfilling.

---

## Configuring the script

All user-editable variables are at the top of `fetch_flights.py`:

| Variable          | Default      | Purpose                                              |
|-------------------|--------------|------------------------------------------------------|
| `AIRPORTS`        | 10 US majors | List of ICAO airport codes to collect                |
| `BACKFILL_TO_DATE`| 2024-01-01   | Oldest date to backfill toward — lower to go further |
| `CREDIT_BUFFER`   | 500          | Credits held in reserve for next day's forward fill  |
| `QUERY_DAYS`      | 2            | Window size per API call (do not change above 2)     |

### Adding airports
Add the ICAO code to the `AIRPORTS` list. On the next run the script will detect
it has no history for the new airport and prioritise backfilling it first until
it matches the depth of the other airports.

---

## Output files (in Google Drive)

| File                    | Contents                                       |
|-------------------------|------------------------------------------------|
| `KATL_2024.csv`         | All KATL departures in 2024                   |
| `KATL_2025.csv`         | All KATL departures in 2025                   |
| `frontiers.json`        | Oldest/latest date fetched per airport (state)|
| `aircraft_cache.json`   | icao24 → aircraft metadata cache              |
| `airlines.csv`          | OpenFlights airline name lookup table         |

### CSV columns

| Column               | Description                              |
|----------------------|------------------------------------------|
| `date`               | UTC date of departure (YYYY-MM-DD)       |
| `departure_airport`  | ICAO code of departing airport           |
| `icao24`             | Aircraft 24-bit Mode S hex address       |
| `callsign`           | Flight callsign                          |
| `carrier_icao`       | 3-letter ICAO airline code               |
| `carrier_name`       | Full airline name                        |
| `departure_time_utc` | Departure timestamp (YYYY-MM-DD HH:MM:SS)|
| `destination_airport`| ICAO code of destination                 |
| `registration`       | Aircraft tail/registration number        |
| `manufacturer`       | Aircraft manufacturer name               |
| `model`              | Aircraft model (e.g. 737-800)            |
| `typecode`           | ICAO aircraft type code (e.g. B738)      |
