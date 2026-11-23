"""Settings in one place. Values come from the environment or .env, with safe defaults."""

import os

from dotenv import load_dotenv

load_dotenv()  # Reads .env from the folder you run the command in, if it exists.

DATABASE = os.getenv("DATABASE_URL") or "data/pipeline.sqlite3"
BRONZE_DIR = os.getenv("BRONZE_DIR") or "data/bronze"

# Where the data comes from: StatsGH, which republishes Bank of Ghana rates (CC BY 4.0).
SOURCE_URL = "https://www.statsgh.com/api/data/bog-fx-rates"

# From the Lab 1 plan: every business day should have this many currency pairs.
EXPECTED_PAIRS_PER_DAY = 19

FIRST_LOAD_DAYS = 30  # How far back the very first run reaches (the API gave about a month).
OVERLAP_DAYS = 3  # Later runs re-read this many recent days, in case values are revised.
