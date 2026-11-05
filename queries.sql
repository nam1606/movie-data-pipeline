-- Q1: Which movie has the highest average rating?
-- A movie with a single 5-star rating would beat everything, so we only
-- count movies with at least 10 ratings.
SELECT
    m.title,
    m.release_year,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(*)                AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
GROUP BY m.movie_id, m.title, m.release_year
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;




-- Q2: What are the top 5 movie genres with the highest average rating?
-- The average is taken over every individual rating given to movies in that genre.
SELECT
    g.genre_name,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(*)                AS num_ratings
FROM genres g
JOIN movie_genres mg ON mg.genre_id = g.genre_id
JOIN ratings r       ON r.movie_id  = mg.movie_id
GROUP BY g.genre_id, g.genre_name
ORDER BY avg_rating DESC
LIMIT 5;


-- Q3: Which director has the most movies in this dataset?
-- Each director is counted separately, so co-directed movies count for every
-- director involved. Only movies already enriched from OMDb have directors, so
-- results improve as more daily ETL runs fill in the data. The top 5 are shown
-- so that ties are visible.
SELECT
    d.director_name,
    COUNT(*) AS num_movies
FROM directors d
JOIN movie_directors md ON md.director_id = d.director_id
GROUP BY d.director_id, d.director_name
ORDER BY num_movies DESC, d.director_name
LIMIT 5;

-- Q4: What is the average rating of movies released each year?
-- Movies with no release year in their title are left out.
SELECT
    m.release_year,
    ROUND(AVG(r.rating), 2) AS avg_rating,
    COUNT(DISTINCT m.movie_id) AS num_movies,
    COUNT(*)                   AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;


-- Q5 (bonus): What is the average rating per decade of release?
-- Uses the engineered "decade" column. Movies with no release year are left out.
SELECT
    m.decade,
    ROUND(AVG(r.rating), 2)    AS avg_rating,
    COUNT(DISTINCT m.movie_id) AS num_movies,
    COUNT(*)                   AS num_ratings
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
WHERE m.decade IS NOT NULL
GROUP BY m.decade
ORDER BY m.decade;


















