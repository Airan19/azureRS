df = spark.read.format("delta").load(f"dbfs:/user/hive/warehouse/rating_time_genre")
fav_movies = []
dbutils.widgets.text("inputUserId", "", "")
inputUserId = dbutils.widgets.get("inputUserId")
print(inputUserId)
fav = df.filter((col("userId") == inputUserId))
for genre in distinct_values:
    column = (
        fav.select("userId", "movieId", "title", col(genre), "rating")
        .filter(col(genre) == 1)
        .agg(F.avg("rating").alias("avg_rating"), F.count("title").alias("total_views"))
        .orderBy(col("total_views").desc(), col("avg_rating").desc())
        .first()
    )
    if column:
        fav_movies.append(
            {
                "genre": genre,
                "total_views": column.total_views,
                "rating": column.avg_rating,
            }
        )

fav_movie_genre = spark.createDataFrame(fav_movies)
fav_movie_genre = fav_movie_genre.orderBy(
    col("total_views").desc(), col("rating").desc()
).first()
favorite_genre = fav_movie_genre.genre
# display(fav_movie_genre)
# def recommend_movies():
user = df.filter((col("userId") == inputUserId))
watched = (
    user.select("userId", "movieId", "title", col(favorite_genre), "rating")
    .filter(col(favorite_genre) == 1)
    .orderBy(col("rating").desc())
)
# watched.show()
print(favorite_genre)
column = (
    df.filter(col(favorite_genre) == 1)
    .groupBy([favorite_genre, "title"])
    .agg(F.count("movieId").alias("movie_count"))
    .orderBy("movie_count", ascending=False)
)
# display(column)
recommendation = column.join(watched, "title", "leftanti")
recommendation = recommendation.drop("movie_count", favorite_genre)
