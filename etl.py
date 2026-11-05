"""ETL pipeline: MovieLens CSVs + OMDb API -> MySQL (movie_db)."""

import argparse
import json
import os
import logging

import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Load settings from the .env file
load_dotenv()

DATA_DIR = "data"
OMDB_API_KEY = os.getenv("OMDB_API_KEY")
CACHE_FILE = "omdb_cache.json"
MAX_OMDB_CALLS_PER_RUN = 990  # free key allows about 1000 calls per day

# Log messages with a timestamp, so we can see what the pipeline is doing
LOG_FILE = "etl.log"

# Log to the screen and to a file, so every run leaves a permanent record
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)
class OmdbAccessError(Exception):
    """Raised when OMDb refuses requests (daily limit reached or invalid API key)."""


def get_engine():
    """Create a database connection using the settings in .env."""
    url = URL.create(
        drivername="mysql+pymysql",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
    )
    return create_engine(url)


# ---------------------------------------------------------------- EXTRACT

def extract_csvs(data_dir=DATA_DIR):
    """Read the MovieLens CSV files from data_dir into pandas DataFrames."""
    movies = pd.read_csv(os.path.join(data_dir, "movies.csv"))
    ratings = pd.read_csv(os.path.join(data_dir, "ratings.csv"))
    links = pd.read_csv(os.path.join(data_dir, "links.csv"), dtype={"imdbId": str})
    log.info("Read %d movies, %d ratings, %d links", len(movies), len(ratings), len(links))
    return movies, ratings, links


def fetch_omdb(imdb_id):
    """Fetch one movie from OMDb by IMDb ID.
    Returns the data dict, {"Response": "False"} if OMDb has no match,
    or None if the request itself failed (so it is retried next run).
    Raises OmdbAccessError if OMDb refuses requests (daily limit or bad key)."""
    try:
        resp = requests.get(
            "https://www.omdbapi.com/",
            params={"apikey": OMDB_API_KEY, "i": imdb_id},
            timeout=10,
        )
        # Read the body first: OMDb reports limit and key problems in the JSON
        # message, and may send them with an HTTP error status such as 401
        data = resp.json()
    except (requests.RequestException, ValueError) as exc:
        log.warning("OMDb request failed for %s: %s", imdb_id, exc)
        return None

    if data.get("Response") != "True":
        error = data.get("Error", "")
        # A refused request is not the same as "movie not found", so stop the run
        if "limit" in error.lower() or "api key" in error.lower():
            raise OmdbAccessError(error)
        # Any other HTTP error is a server problem, so retry on the next run
        if resp.status_code >= 400:
            log.warning("OMDb returned HTTP %d for %s: %s", resp.status_code, imdb_id, error)
            return None
        log.info("OMDb has no match for %s: %s", imdb_id, error)
        return {"Response": "False"}
    return data


def load_cache():
    """Load previously fetched OMDb responses from disk (empty dict if none yet)."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    """Write OMDb responses to disk so they survive between runs."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f)


def enrich_movies(movies_clean, limit=None):
    """Fetch OMDb data for every movie, using the cache and a per-run call limit.
    Returns the cache dict (imdb_id -> OMDb response)."""
    cache = load_cache()
    max_calls = limit if limit is not None else MAX_OMDB_CALLS_PER_RUN
    calls = 0

    for imdb_id in movies_clean["imdb_id"].dropna():
        if imdb_id in cache:
            continue  # already fetched in an earlier run
        if calls >= max_calls:
            log.warning("Reached the limit of %d API calls; re-run later to continue", max_calls)
            break

        try:
            result = fetch_omdb(imdb_id)
        except OmdbAccessError as exc:
            log.warning("OMDb refused the request (%s). Stopping; re-run tomorrow.", exc)
            break
        calls += 1
        if result is not None:  # None means the request failed, so retry next run
            cache[imdb_id] = result

        if calls % 100 == 0:
            save_cache(cache)  # save progress regularly
            log.info("Fetched %d movies so far in this run", calls)

    save_cache(cache)
    log.info("OMDb calls this run: %d | movies in cache: %d", calls, len(cache))
    return cache


# -------------------------------------------------------------- TRANSFORM

def clean_movies(movies, links):
    """Clean movie data: split year out of the title, add decade and IMDb ID."""
    df = movies.merge(links[["movieId", "imdbId"]], on="movieId", how="left")

    df["title"] = df["title"].str.strip()

    # Titles look like "Toy Story (1995)", so pull the 4-digit year from the end
    year = df["title"].str.extract(r"\((\d{4})\)\s*$")[0]
    df["release_year"] = pd.to_numeric(year, errors="coerce").astype("Int64")

    # Remove the year from the title text
    df["title"] = df["title"].str.replace(r"\s*\(\d{4}\)\s*$", "", regex=True)

    # Decade, for example 1995 -> 1990
    df["decade"] = (df["release_year"] // 10 * 10).astype("Int64")

    # OMDb expects IDs like tt0114709
    df["imdb_id"] = "tt" + df["imdbId"]

    df = df.rename(columns={"movieId": "movie_id"})
    return df[["movie_id", "title", "release_year", "decade", "imdb_id", "genres"]]


def clean_ratings(ratings):
    """Clean ratings: rename columns, convert timestamps to dates, drop bad rows."""
    df = ratings.rename(columns={"userId": "user_id", "movieId": "movie_id"})

    # timestamp is seconds since 1970, so convert it to a real datetime
    df["rated_at"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df = df.dropna(subset=["user_id", "movie_id", "rating"])

    # One rating per user per movie, matching the table's primary key
    df = df.drop_duplicates(subset=["user_id", "movie_id"], keep="last")

    return df[["user_id", "movie_id", "rating", "rated_at"]]


def build_genres(movies_clean):
    """Split the 'Action|Comedy' text into a genres list and movie-genre pairs."""
    pairs = movies_clean[["movie_id", "genres"]].copy()
    pairs["genre_name"] = pairs["genres"].str.split("|")
    pairs = pairs.explode("genre_name")

    # MovieLens uses this placeholder for movies with no genre
    pairs = pairs[pairs["genre_name"] != "(no genres listed)"]
    pairs = pairs.dropna(subset=["genre_name"])
    pairs["genre_name"] = pairs["genre_name"].str.strip()
    pairs = pairs.drop_duplicates(subset=["movie_id", "genre_name"])

    genre_names = sorted(pairs["genre_name"].unique())
    return genre_names, pairs[["movie_id", "genre_name"]]


def merge_omdb(movies_clean, cache):
    """Add director, plot, box office and an omdb_found flag from the OMDb cache."""

    def get_field(imdb_id, field):
        data = cache.get(imdb_id)
        if not data or data.get("Response") != "True":
            return None
        value = data.get(field)
        return None if value in (None, "N/A") else value

    df = movies_clean.copy()
    df["director"] = df["imdb_id"].map(lambda i: get_field(i, "Director"))
    df["plot"] = df["imdb_id"].map(lambda i: get_field(i, "Plot"))

    # Box office arrives as text like "$229,947,062", so keep only the digits
    box = df["imdb_id"].map(lambda i: get_field(i, "BoxOffice"))
    df["box_office"] = pd.to_numeric(
        box.str.replace(r"[^\d]", "", regex=True), errors="coerce"
    ).astype("Int64")

    def lookup_status(imdb_id):
        """True = found in OMDb, False = OMDb has no match, None = not fetched yet."""
        if imdb_id not in cache:
            return None
        return cache[imdb_id].get("Response") == "True"

    df["omdb_found"] = df["imdb_id"].map(lookup_status)
    return df


# ------------------------------------------------------------------- LOAD

def to_records(df):
    """Convert a DataFrame to a list of dicts, turning NaN/NA values into None (NULL)."""
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict(orient="records")


def load_movies(engine, df):
    """Insert movies, or update them if the movie_id already exists (idempotent)."""
    sql = text("""
        INSERT INTO movies
            (movie_id, title, release_year, decade, imdb_id, director, plot, box_office, omdb_found)
        VALUES
            (:movie_id, :title, :release_year, :decade, :imdb_id, :director, :plot, :box_office, :omdb_found)
        AS new
                ON DUPLICATE KEY UPDATE
            title = new.title,
            release_year = new.release_year,
            decade = new.decade,
            imdb_id = new.imdb_id,
            director = COALESCE(new.director, movies.director),
            plot = COALESCE(new.plot, movies.plot),
            box_office = COALESCE(new.box_office, movies.box_office),
            omdb_found = COALESCE(new.omdb_found, movies.omdb_found)    
""")
    cols = ["movie_id", "title", "release_year", "decade", "imdb_id",
            "director", "plot", "box_office", "omdb_found"]
    with engine.begin() as conn:
        conn.execute(sql, to_records(df[cols]))
    log.info("Loaded %d movies", len(df))


def load_genres(engine, genre_names):
    """Insert genre names, skipping any that already exist (idempotent)."""
    sql = text("INSERT IGNORE INTO genres (genre_name) VALUES (:genre_name)")
    with engine.begin() as conn:
        conn.execute(sql, [{"genre_name": g} for g in genre_names])
    log.info("Loaded %d genres", len(genre_names))


def load_movie_genres(engine, pairs):
    """Link movies to genres, skipping links that already exist (idempotent)."""
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT genre_id, genre_name FROM genres")).fetchall()
        genre_ids = {name: gid for gid, name in rows}

        records = [
            {"movie_id": int(movie_id), "genre_id": genre_ids[name]}
            for movie_id, name in zip(pairs["movie_id"], pairs["genre_name"])
        ]
        sql = text("INSERT IGNORE INTO movie_genres (movie_id, genre_id) VALUES (:movie_id, :genre_id)")
        conn.execute(sql, records)
    log.info("Loaded %d movie-genre links", len(records))


def load_ratings(engine, df, chunk_size=5000):
    """Insert ratings, or update them if (user_id, movie_id) already exists (idempotent)."""
    sql = text("""
        INSERT INTO ratings (user_id, movie_id, rating, rated_at)
        VALUES (:user_id, :movie_id, :rating, :rated_at)
        AS new
        ON DUPLICATE KEY UPDATE
            rating = new.rating,
            rated_at = new.rated_at
    """)
    with engine.begin() as conn:
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            conn.execute(sql, to_records(chunk))
    log.info("Loaded %d ratings", len(df))



def build_directors(movies_final):
    """Split 'Name One, Name Two' into a directors list and movie-director pairs."""
    pairs = movies_final[["movie_id", "director"]].dropna(subset=["director"]).copy()
    pairs["director_name"] = pairs["director"].str.split(",")
    pairs = pairs.explode("director_name")

    # Remove notes in brackets, such as "Name (co-director)", then extra spaces
    pairs["director_name"] = (
        pairs["director_name"].str.replace(r"\s*\(.*?\)", "", regex=True).str.strip()
    )
    pairs = pairs[pairs["director_name"] != ""]
    pairs = pairs.drop_duplicates(subset=["movie_id", "director_name"])

    director_names = sorted(pairs["director_name"].unique())
    return director_names, pairs[["movie_id", "director_name"]]


def load_directors(engine, director_names):
    """Insert director names, skipping any that already exist (idempotent)."""
    sql = text("INSERT IGNORE INTO directors (director_name) VALUES (:director_name)")
    with engine.begin() as conn:
        conn.execute(sql, [{"director_name": d} for d in director_names])
    log.info("Loaded %d directors", len(director_names))


def load_movie_directors(engine, pairs):
    """Link movies to directors, skipping links that already exist (idempotent).
    The director_id is looked up inside MySQL, so names are matched with the
    database's own collation (for example "Ishirô Honda" matches "Ishiro Honda")."""
    records = [
        {"movie_id": int(movie_id), "director_name": name}
        for movie_id, name in zip(pairs["movie_id"], pairs["director_name"])
    ]
    sql = text("""
        INSERT IGNORE INTO movie_directors (movie_id, director_id)
        SELECT :movie_id, director_id FROM directors WHERE director_name = :director_name
    """)
    with engine.begin() as conn:
        conn.execute(sql, records)
    log.info("Loaded %d movie-director links", len(records))



# ------------------------------------------------------------------- MAIN

def main(data_dir=DATA_DIR):
    engine = get_engine()
    with engine.connect() as conn:
        db_name = conn.execute(text("SELECT DATABASE()")).scalar()
        log.info("Connected to database: %s", db_name)

    # Extract
    movies, ratings, links = extract_csvs(data_dir)

    # Transform
    movies_clean = clean_movies(movies, links)
    ratings_clean = clean_ratings(ratings)
    genre_names, movie_genre_pairs = build_genres(movies_clean)

    # Enrich with OMDb (cached, and limited to the daily API allowance)
    cache = enrich_movies(movies_clean)
    movies_final = merge_omdb(movies_clean, cache)
    log.info("Movies with OMDb details so far: %d of %d",
     movies_final["omdb_found"].eq(True).sum(), len(movies_final))

    director_names, movie_director_pairs =build_directors(movies_final)

    # Load (movies first, because the other tables point to them)
    load_movies(engine, movies_final)
    load_genres(engine, genre_names)
    load_movie_genres(engine, movie_genre_pairs)
    load_directors(engine, director_names)
    load_movie_directors(engine, movie_director_pairs)
    load_ratings(engine, ratings_clean)

    log.info("Pipeline finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Movie ETL pipeline: MovieLens CSVs + OMDb API -> MySQL"
    )
    parser.add_argument(
        "--data-dir",
        default=DATA_DIR,
        help="folder containing movies.csv, ratings.csv and links.csv (default: data)",
    )
    args = parser.parse_args()
    main(args.data_dir)
    