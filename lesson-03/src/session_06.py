from pyspark import SparkContext
from functools import reduce
import datetime

def load_ratings_with_timestamp(sc, ratings_paths):
    rdds = [sc.textFile(p) for p in ratings_paths]
    all_ratings = reduce(lambda a, b: a.union(b), rdds)
    return all_ratings.map(lambda line: [x.strip() for x in line.split(',')]) \
                      .map(lambda x: (int(x[3]), float(x[2])))   # (timestamp, rating)

def timestamp_to_year(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).year

def year_rating_pair(rdd):
    # Trả về RDD (year, rating)
    return rdd.map(lambda x: (timestamp_to_year(x[0]), x[1]))

def avg_by_year(rdd):
    # Tính tổng và đếm
    sums_counts = rdd.map(lambda x: (x[0], (x[1], 1))) \
                     .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # Tính trung bình
    return sums_counts.mapValues(lambda x: (x[0] / x[1], x[1]))