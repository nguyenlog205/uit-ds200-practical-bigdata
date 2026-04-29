from pyspark import SparkContext

def get_user_age_group(sc, users_path):
    """
    Đọc users.txt, thêm cột nhóm tuổi.
    Phân loại:
        1: < 18
        2: 18-25
        3: 26-35
        4: 36-50
        5: > 50
    Trả về RDD (UserID, AgeGroup)
    """
    def age_to_group(age):
        if age < 18:
            return 1
        elif age <= 25:
            return 2
        elif age <= 35:
            return 3
        elif age <= 50:
            return 4
        else:
            return 5

    users_rdd = sc.textFile(users_path)
    return users_rdd.map(lambda line: [x.strip() for x in line.split(',')]) \
                    .map(lambda x: (int(x[0]), age_to_group(int(x[2]))))

def load_ratings_with_user(sc, ratings_paths):
    """Đọc ratings, trả về RDD (UserID, MovieID, Rating)"""
    from functools import reduce
    rdds = [sc.textFile(p) for p in ratings_paths]
    all_ratings = reduce(lambda a, b: a.union(b), rdds)
    return all_ratings.map(lambda line: [x.strip() for x in line.split(',')]) \
                      .map(lambda x: (int(x[0]), int(x[1]), float(x[2])))

def join_ratings_agegroup(ratings_triple, user_agegroup):
    """
    ratings_triple: (UserID, MovieID, Rating)
    user_agegroup: (UserID, AgeGroup)
    Trả về RDD (MovieID, (Rating, AgeGroup))
    """
    ratings_by_user = ratings_triple.map(lambda x: (x[0], (x[1], x[2])))
    joined = ratings_by_user.join(user_agegroup)   # (UserID, ((MovieID, Rating), AgeGroup))
    return joined.map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1])))

def avg_rating_by_agegroup_per_movie(joined_rdd):
    """
    joined_rdd: (MovieID, (Rating, AgeGroup))
    Tính trung bình rating cho mỗi (MovieID, AgeGroup)
    Trả về RDD (MovieID, dict {age_group: avg_rating})
    """
    # Chuyển thành ( (MovieID, AgeGroup), (Rating, 1) )
    mapped = joined_rdd.map(lambda x: ((x[0], x[1][1]), (x[1][0], 1)))
    reduced = mapped.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # Tính trung bình
    avg_per_pair = reduced.mapValues(lambda x: x[0] / x[1])
    # Gom theo MovieID -> dict
    return avg_per_pair.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                       .groupByKey() \
                       .mapValues(lambda grp: dict(grp))