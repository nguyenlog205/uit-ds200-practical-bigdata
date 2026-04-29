from pyspark import SparkContext

def load_ratings_with_user(sc, ratings_paths):
    """Đọc ratings, trả về RDD (UserID, Rating)"""
    from functools import reduce
    rdds = [sc.textFile(p) for p in ratings_paths]
    all_ratings = reduce(lambda a, b: a.union(b), rdds)
    return all_ratings.map(lambda line: [x.strip() for x in line.split(',')]) \
                      .map(lambda x: (int(x[0]), float(x[2])))

def get_user_occupation_id(sc, users_path):
    """Đọc users.txt, trả về RDD (UserID, OccupationID)"""
    users_rdd = sc.textFile(users_path)
    return users_rdd.map(lambda line: [x.strip() for x in line.split(',')]) \
                    .map(lambda x: (int(x[0]), int(x[3])))   # OccupationID

def join_ratings_occupation(ratings_user_rdd, user_occ_rdd):
    """
    ratings_user_rdd: (UserID, Rating)
    user_occ_rdd: (UserID, OccupationID)
    Trả về RDD (OccupationID, Rating)
    """
    joined = ratings_user_rdd.join(user_occ_rdd)
    return joined.map(lambda x: (x[1][1], x[1][0]))  # (OccID, Rating)

def avg_rating_by_occupation(occ_rating_rdd):
    """
    occ_rating_rdd: (OccupationID, Rating)
    Trả về RDD (OccupationID, (avg_rating, count))
    """
    mapped = occ_rating_rdd.mapValues(lambda r: (r, 1))
    reduced = mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return reduced.mapValues(lambda x: (x[0] / x[1], x[1]))