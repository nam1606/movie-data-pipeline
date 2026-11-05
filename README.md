# Movie Data Pipeline

An ETL pipeline that combines the MovieLens "small" dataset with details from the OMDb API, cleans and transforms it with Python, loads it into MySQL, and answers four analytical questions with SQL.

## Overview

1. **Extract**: reads `movies.csv`, `ratings.csv` and `links.csv` (MovieLens) and fetches extra details (director, plot, box office) from the OMDb API using each movie's IMDb ID.

2. **Transform**: cleans the data, extracts the release year and decade from the title, converts timestamps to dates, converts box office text to numbers, and splits the pipe-separated genres into one row per movie-genre pair.

3. **Load**: writes the data into six MySQL tables using upserts, so the script can be run any number of times without creating duplicates.

4. **Analyse**: `queries.sql` answers the four questions from the assignment, plus a bonus query that uses the engineered `decade` column.

## Project structure

```
etl.py             the ETL pipeline
schema.sql         CREATE TABLE statements
queries.sql        the analytical queries (four required, plus one bonus)
requirements.txt   Python dependencies
tests/             automated tests (pytest)
.env.example       template for the settings file
README.md          this file
```

## How to run

**1. Prerequisites:** Python 3.10 or newer, and MySQL Server 8.0.19 or newer running locally.

**2. Create the database**

```sql
CREATE DATABASE movie_db;
```

**3. Create the tables**

```
mysql -u root -p movie_db < schema.sql
```

**4. Install dependencies**

```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

**5. Get the data**

Download `ml-latest-small.zip` from https://grouplens.org/datasets/movielens/latest/ and place `movies.csv`, `ratings.csv` and `links.csv` in a folder named `data` inside the project folder. To use a different folder, run `python etl.py --data-dir PATH`.

**6. Configure settings**

Copy `.env.example` to `.env` and fill in your MySQL password and a free OMDb API key (https://www.omdbapi.com/apikey.aspx). The `.env` file is ignored by Git, so secrets are never committed.

**7. Run the pipeline**

```
python etl.py
```

**8. Run the queries**

```
mysql -u root -p movie_db < queries.sql
```

## Important: OMDb daily limit

The free OMDb key allows about 1,000 requests per day, and the dataset has 9,742 movies. The script therefore makes at most 990 API calls per run and saves every response in a local cache (`omdb_cache.json`). Movies already in the cache are never requested again. Run `python etl.py` once per day, and about 10 runs will enrich every movie. Until then, movies that have not been fetched yet have empty director, plot and box office values. All other tables are always complete.

## Running the tests

The transform logic has automated tests that need no database or API key. Install pytest and run:

```
pip install pytest
python -m pytest tests
```

They cover the movie, rating, genre and director cleaning functions, including the three-state omdb_found flag.

## Data model

| Table | Description | Primary key |

|---|---|---|

| `movies` | One row per movie: title, release year, decade, IMDb ID, and OMDb details | `movie_id` |

| `genres` | One row per genre name | `genre_id` |

| `movie_genres` | Links movies and genres (many-to-many) | `movie_id` + `genre_id` |

| `ratings` | One row per user rating | `user_id` + `movie_id` |

| `directors` | One row per director name | `director_id` |

| `movie_directors` | Links movies and directors (many-to-many) | `movie_id` + `director_id` |

Design choices:

- Genres are stored in their own table instead of a text column, because a movie has many genres and a genre has many movies. This also makes genre queries simple and fast.

- OMDb returns co-directors as one comma-separated text value, so directors get their own table and a link table, like genres. This lets a movie have several directors, and lets Query 3 credit every co-director instead of treating "Name One, Name Two" as one person.

- Primary keys on every table are what make the load idempotent.

- `omdb_found` has three values: NULL (not fetched yet), TRUE (found in OMDb) and FALSE (OMDb has no match), so "not found" can be told apart from "not fetched yet".

- `decade` is a derived column, added as the bonus feature-engineering step.

## Assumptions and design decisions

- **Matching movies:** movies are looked up by IMDb ID (from `links.csv`) rather than by title, because titles often differ between sources.

- **Missing data:** movies with no year in the title keep an empty release year. OMDb values of `N/A` become NULL. Movies OMDb does not know are stored with `omdb_found = FALSE`, and movies not fetched yet keep `omdb_found = NULL`.

- **Failed requests:** a network error or timeout is logged and skipped without crashing, and is not cached, so it is retried on the next run. A "movie not found" response is cached so it is not requested again.

- **Query 1 (highest average rating):** only movies with at least 10 ratings are counted, otherwise a movie with a single 5-star rating would win.

- **Query 2 (top 5 genres):** the average is taken over every individual rating given to movies in that genre. A movie with several genres counts toward each of them, and ties (such as Drama and Crime, both 3.66) are listed in arbitrary order.

- **Query 3 (director with the most movies):** each director is counted separately, so a co-directed movie counts for every director involved. Only movies already enriched from OMDb have directors, so this result changes as more daily runs complete.

- **Query 4 (average rating per year):** recent years have few ratings, so their averages are less reliable.

- **Query 5 (bonus, average rating per decade):** uses the engineered `decade` column. The earliest decades have very few ratings (the 1900s and 1910s have 8 each), so their averages are less reliable.

## Challenges and how I solved them

- **API rate limit:** the free key covers only about a tenth of the movies per day. I added a persistent cache, a per-run call limit and regular saves, so the pipeline can be stopped and restarted safely.

- **Unreliable requests:** some OMDb calls time out. Every request has a timeout and error handling, so one bad call never stops the run.

- **Messy source data:** years are inside titles, genres are packed in one text cell, timestamps are Unix numbers and box office is text like `$229,947,062`. Each is handled in a dedicated transform function.

- **Safe re-runs:** every load uses `INSERT ... ON DUPLICATE KEY UPDATE` or `INSERT IGNORE`, and I tested that loading the same data twice does not change the row counts.

- **Secrets:** the database password and API key live in a `.env` file that is excluded from Git.
- **Accented names:** MySQL matches text ignoring accents, so two spellings of a director (such as Ishirô Honda and Ishiro Honda) are the same row. A name lookup in Python crashed on the skipped spelling. I fixed it by looking up the director ID inside MySQL, so names are matched the way the database matches them.

## Possible improvements for production

1. Run the pipeline on a schedule with an orchestrator such as Airflow, with retries and alerts.

2. Call the API concurrently with rate limiting and exponential backoff, and use a paid API tier.

3. Move from MySQL to a cloud warehouse, and load incrementally instead of reloading everything.

4. Add automated tests and data quality checks, such as row counts and null-rate checks.

