CREATE TABLE IF NOT EXISTS movies (
    movie_id      INT PRIMARY KEY,
    title         VARCHAR(255) NOT NULL,
    release_year  SMALLINT NULL,
    decade        SMALLINT NULL,
    imdb_id       VARCHAR(12) NULL UNIQUE,
    director      VARCHAR(255) NULL,
    plot          TEXT NULL,
    box_office    BIGINT NULL,
    omdb_found    BOOLEAN NULL  -- NULL = not fetched yet, TRUE = found in OMDb, FALSE = OMDb has no match
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id    INT AUTO_INCREMENT PRIMARY KEY,
    genre_name  VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id  INT NOT NULL,
    genre_id  INT NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id   INT NOT NULL,
    movie_id  INT NOT NULL,
    rating    DECIMAL(2,1) NOT NULL  CHECK (rating BETWEEN 0.5 AND 5.0),
    rated_at  DATETIME NULL,
    PRIMARY KEY (user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);


CREATE TABLE IF NOT EXISTS directors (
    director_id    INT AUTO_INCREMENT PRIMARY KEY,
    director_name  VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id     INT NOT NULL,
    director_id  INT NOT NULL,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY (director_id) REFERENCES directors(director_id)
);