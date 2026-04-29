from pyspark import SparkContext

def get_movie_genres(sc, movies_path):
    movies_rdd = sc.textFile(movies_path)
    return movies_rdd.map(lambda line: [x.strip() for x in line.split(',')]) \
                     .map(lambda x: (int(x[0]), x[2].split('|')))

def explode_genres_ratings(ratings_rdd, movie_genres_rdd):
    joined = ratings_rdd.join(movie_genres_rdd)
    return joined.flatMap(lambda x: [(genre, x[1][0]) for genre in x[1][1]])

def avg_rating_by_genre(genre_rating_rdd):
    mapped = genre_rating_rdd.mapValues(lambda r: (r, 1))
    reduced = mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    avg_cnt = reduced.mapValues(lambda x: (x[0] / x[1], x[1]))
    return avg_cnt