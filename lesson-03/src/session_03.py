from pyspark import SparkContext

def get_user_gender(sc, users_path):
    """
    Đọc users.txt, trả về RDD (UserID, Gender)
    Gender: 'M' hoặc 'F'
    """
    users_rdd = sc.textFile(users_path)
    return users_rdd.map(lambda line: [x.strip() for x in line.split(',')]) \
                    .map(lambda x: (int(x[0]), x[1]))   # (UserID, Gender)

def join_ratings_with_gender(ratings_rdd, user_gender_rdd):
    """
    ratings_rdd: (UserID, Rating, MovieID)… Thực tế load_ratings từ session_01 trả về (MovieID, Rating)
    Nhưng để join được theo UserID, ta cần ratings dạng (UserID, (MovieID, Rating))
    Hàm này sẽ biến đổi ratings_rdd về dạng (UserID, (MovieID, Rating)) rồi join với (UserID, Gender)
    Kết quả: RDD (MovieID, (Rating, Gender))
    """
    # Giả sử ratings_rdd hiện tại có dạng (MovieID, Rating) - cần chuyển về (UserID, (MovieID, Rating))
    # Nhưng load_ratings trong session_01 chỉ trả về (MovieID, Rating) - mất UserID!
    # Do đó ta cần một hàm load_ratings_with_user riêng cho bài này.
    # Tuy nhiên để tái sử dụng, ta sẽ tạo một hàm mới load_full_ratings.
    # Tạm thời, giả sử đầu vào ratings_rdd đã ở dạng (UserID, MovieID, Rating)
    pass

# --- Các hàm bổ trợ cần thiết ---
def load_ratings_with_user(sc, ratings_paths):
    """Đọc ratings, trả về RDD (UserID, MovieID, Rating)"""
    from functools import reduce
    rdds = [sc.textFile(p) for p in ratings_paths]
    all_ratings = reduce(lambda a, b: a.union(b), rdds)
    return all_ratings.map(lambda line: [x.strip() for x in line.split(',')]) \
                      .map(lambda x: (int(x[0]), int(x[1]), float(x[2])))   # (UserID, MovieID, Rating)

def join_ratings_gender(ratings_triple_rdd, user_gender_rdd):
    """
    ratings_triple_rdd: (UserID, MovieID, Rating)
    user_gender_rdd: (UserID, Gender)
    Kết quả: RDD (MovieID, (Rating, Gender))
    """
    # Chuyển ratings về dạng (UserID, (MovieID, Rating))
    ratings_by_user = ratings_triple_rdd.map(lambda x: (x[0], (x[1], x[2])))
    # Join
    joined = ratings_by_user.join(user_gender_rdd)   # (UserID, ((MovieID, Rating), Gender))
    # Bỏ UserID, lấy (MovieID, (Rating, Gender))
    result = joined.map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1])))
    return result

def avg_rating_by_gender_per_movie(joined_rdd):
    """
    joined_rdd: (MovieID, (Rating, Gender))
    Tính trung bình rating cho mỗi (MovieID, Gender)
    Trả về: RDD (MovieID, (avg_M, avg_F, count_M, count_F))
    """
    # Thêm 1 vào giá trị đếm
    mapped = joined_rdd.map(lambda x: (x[0], (x[1][0], x[1][1], 1)))   # (MovieID, (rating, gender, 1))
    
    # Tách riêng theo gender để tính tổng
    # Cách 1: dùng groupByKey rồi tính, nhưng nên dùng aggregateByKey hiệu quả hơn
    def seq_op(acc, val):
        # acc: [sum_M, count_M, sum_F, count_F]
        # val: (rating, gender, 1)
        rating, gender, _ = val
        if gender == 'M':
            return (acc[0] + rating, acc[1] + 1, acc[2], acc[3])
        else:
            return (acc[0], acc[1], acc[2] + rating, acc[3] + 1)
    
    def comb_op(acc1, acc2):
        return (acc1[0] + acc2[0], acc1[1] + acc2[1],
                acc1[2] + acc2[2], acc1[3] + acc2[3])
    
    # Giá trị khởi tạo: (sum_M, count_M, sum_F, count_F)
    zero = (0.0, 0, 0.0, 0)
    aggregated = mapped.aggregateByKey(zero, seq_op, comb_op)
    
    # Tính trung bình
    def compute_avg(agg):
        sum_M, cnt_M, sum_F, cnt_F = agg
        avg_M = sum_M / cnt_M if cnt_M > 0 else None
        avg_F = sum_F / cnt_F if cnt_F > 0 else None
        return (avg_M, avg_F, cnt_M, cnt_F)
    
    return aggregated.mapValues(compute_avg)