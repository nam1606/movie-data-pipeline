import pandas as pd

import etl


def test_clean_movies_extracts_year_decade_and_imdb_id():
    movies = pd.DataFrame({
        "movieId": [1, 2],
        "title": ["Toy Story (1995)", "No Year Movie"],
        "genres": ["Adventure|Animation", "Comedy"],
    })
    links = pd.DataFrame({"movieId": [1, 2], "imdbId": ["0114709", "0000001"]})

    result = etl.clean_movies(movies, links)

    toy = result[result["movie_id"] == 1].iloc[0]
    assert toy["title"] == "Toy Story"
    assert toy["release_year"] == 1995
    assert toy["decade"] == 1990
    assert toy["imdb_id"] == "tt0114709"

    # A title without a year must give empty values, not an error
    no_year = result[result["movie_id"] == 2].iloc[0]
    assert pd.isna(no_year["release_year"])
    assert pd.isna(no_year["decade"])


def test_build_genres_splits_pipes_and_drops_placeholder():
    movies_clean = pd.DataFrame({
        "movie_id": [1, 2],
        "genres": ["Adventure|Comedy", "(no genres listed)"],
    })

    names, pairs = etl.build_genres(movies_clean)

    assert names == ["Adventure", "Comedy"]
    assert len(pairs) == 2
    assert set(pairs["movie_id"]) == {1}  # movie 2 has no genre rows


def test_build_directors_splits_co_directors():
    movies_final = pd.DataFrame({
        "movie_id": [1, 2, 3],
        "director": [
            "Lana Wachowski, Lilly Wachowski",
            None,
            "Jane Doe (co-director), John Roe",
        ],
    })

    names, pairs = etl.build_directors(movies_final)

    assert names == ["Jane Doe", "John Roe", "Lana Wachowski", "Lilly Wachowski"]
    assert len(pairs) == 4  # movie 2 has no director, so it gets no rows
    assert 2 not in set(pairs["movie_id"])
def test_merge_omdb_flag_has_three_states_and_parses_fields():
    movies_clean = pd.DataFrame({
        "movie_id": [1, 2, 3],
        "imdb_id": ["tt1", "tt2", "tt3"],
    })
    cache = {
        "tt1": {"Response": "True", "Director": "A B", "Plot": "N/A", "BoxOffice": "$1,234"},
        "tt2": {"Response": "False"},  # OMDb looked and has no match
        # tt3 is not in the cache, so it was never fetched
    }

    result = etl.merge_omdb(movies_clean, cache)

    found = result[result["imdb_id"] == "tt1"].iloc[0]
    assert found["omdb_found"] == True  # noqa: E712
    assert found["director"] == "A B"
    assert found["box_office"] == 1234   # "$1,234" becomes a number
    assert pd.isna(found["plot"])        # "N/A" becomes NULL

    not_found = result[result["imdb_id"] == "tt2"].iloc[0]
    assert not pd.isna(not_found["omdb_found"])
    assert not_found["omdb_found"] == False  # noqa: E712
    assert pd.isna(not_found["director"])

    not_fetched = result[result["imdb_id"] == "tt3"].iloc[0]
    assert pd.isna(not_fetched["omdb_found"])  # NULL means not fetched yet


def test_clean_ratings_converts_dates_and_drops_bad_rows():
    ratings = pd.DataFrame({
        "userId": [1, 1, 2],
        "movieId": [10, 10, 10],
        "rating": [3.0, 4.0, None],      # row 3 has a missing rating
        "timestamp": [0, 86400, 0],      # seconds since 1970
    })

    result = etl.clean_ratings(ratings)

    assert len(result) == 1                        # the missing rating is dropped, and
    assert result.iloc[0]["rating"] == 4.0         # the duplicate keeps the last rating
    assert result.iloc[0]["rated_at"] == pd.Timestamp("1970-01-02")