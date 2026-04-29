# src/analysis.py
from pyspark import SparkContext

def get_movie_titles(sc, movies_path):
    """Đọc movies.txt → RDD cặp (MovieID, Title)"""
    movies_rdd = sc.textFile(movies_path)
    return movies_rdd.map(lambda line: line.split(',')) \
                     .map(lambda x: (int(x[0]), x[1]))

def load_ratings(sc, ratings_paths):
    """Đọc nhiều file ratings, trả về RDD (MovieID, Rating)"""
    rdd_list = [sc.textFile(path) for path in ratings_paths]
    all_ratings = rdd_list[0].union(*rdd_list[1:]) if len(rdd_list) > 1 else rdd_list[0]
    return all_ratings.map(lambda line: line.split(',')) \
                      .map(lambda x: (int(x[1]), float(x[2])))

def avg_rating_per_movie(ratings_rdd, min_ratings=50):
    """Tính (MovieID → (avg_rating, count)) và lọc phim có >= min_ratings"""
    # (MovieID, (rating, 1))
    mapped = ratings_rdd.mapValues(lambda r: (r, 1))
    # reduce: (sum_rating, count)
    reduced = mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # tính avg, lọc
    avg_cnt = reduced.mapValues(lambda x: (x[0]/x[1], x[1]))
    return avg_cnt.filter(lambda x: x[1][1] >= min_ratings)

def best_movie(avg_rdd, titles_rdd):
    """Tìm phim có avg cao nhất, trả về (title, avg_rating)"""
    # (avg, movie_id)
    best = avg_rdd.map(lambda x: (x[1][0], x[0])).max()
    title = titles_rdd.lookup(best[1])[0]
    return title, best[0]