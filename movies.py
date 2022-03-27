

from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import split, when, col, array_contains, explode, rank, collect_list, avg, count,round, array_repeat
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from re import match

spark = SparkSession \
    .builder \
    .appName("PySpark Movie Database") \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \
sc = spark.sparkContext

MOVIE_DB_DIRECTORY = "ml-latest-small/"

#read the data frames
links = spark.read.csv(MOVIE_DB_DIRECTORY+"links.csv",inferSchema='True',header=True)



ratings = spark.read.csv(MOVIE_DB_DIRECTORY+"ratings.csv",inferSchema='True',header=True)

tags = spark.read.csv(MOVIE_DB_DIRECTORY+"tags.csv",inferSchema='True',header=True)

#reviews joined with movie 
#if exists(MOVIE_DB_DIRECTORY+"reviews"):
#    reviews = spark.read.(MOVIE_DB_DIRECTORY+"reviews")
#else:
#all movies
reviews = spark.read.csv(MOVIE_DB_DIRECTORY+"movies.csv",inferSchema='True',header=True)\
    .withColumn("genre_array",split(col("genres"),"[|]"))\
    .drop("genres")\
    .withColumn("year",split(col("title"),"[(](?=.....$)|[)] ?$").getItem(1).cast('int'))\
    .join(ratings, ["movieId"], "left")\
    .na.fill(0)
#reviews.write.json(MOVIE_DB_DIRECTORY+"reviews")

#average review score
reviews_avg = reviews.withColumn("dummy", when(0 < col("userId"),1).otherwise(0))\
    .groupBy("movieId","title",col("genre_array").alias("genres"),"year")\
    .agg(
        round(avg(col("rating")),1).alias("avg_rating"),
        F.sum(col("dummy")).alias("watches") #uses dummy column so that movies with zero reviews don't get counted 
    ).drop_duplicates()

#users and counts of genres they watched
user_genres = reviews.where(0<col("userId"))\
        .select("userId",explode(reviews.genre_array).alias("split"))\
        .groupBy("userId","split")\
        .count()



window = Window.partitionBy(user_genres['userId']).orderBy(user_genres['count'].desc())
favorites = user_genres.select('*', rank()
        .over(window)
        .alias('rank')) \
        .filter(col('rank') <= 5) 


#vector assembler 

CLASS_FILE = "classes.csv"
if exists(MOVIE_DB_DIRECTORY+CLASS_FILE):
    clusters = spark.read.csv(MOVIE_DB_DIRECTORY+CLASS_FILE,inferSchema='True',header=True)
else:
    pivoted = favorites\
        .select("userId","rank","split",explode(array_repeat("rank",col("rank"))).alias("c"))\
        .groupBy("userId").pivot("split").count()\
        .na.fill(0)

    assemble=VectorAssembler(inputCols=pivoted.columns[1:],outputCol="features")
    assembled_data=assemble.transform(pivoted)

    scale=StandardScaler(inputCol='features',outputCol='standardized')
    data_scale=scale.fit(assembled_data)
    data_scale_output=data_scale.transform(assembled_data)

    kmeans = KMeans(k=10, seed=69)  # 2 clusters here
    model = kmeans.fit(data_scale_output)#.select('features'))

    clusters = model.transform(data_scale_output).select("userId",col("prediction").alias("class"))
    clusters.write.csv(MOVIE_DB_DIRECTORY+CLASS_FILE,header=True)


"""
get all movies watched by people in id_list
"""
def getWatched(id_list):
    data = reviews.where(( 0 < col("userId")) & col("userId").isin(id_list))\
        .groupBy("movieId","title","rating","genre_array").agg(collect_list('userId').alias('watchers'))\
        .select("watchers","movieId","title",col("genre_array").alias("genres"))
    genre_count = user_genres.where(col("userId").isin(id_list)).groupBy("split")\
        .count().count()#total number of genres
    movie_count = data.groupBy("movieId").count().count()
    msg = "Number of movies: {}, Number of genres {}".format(movie_count,genre_count)

    return data, msg

    
            
"""
get all movies in genre
"""
def get_genre(genre_list):
    data = reviews_avg.select("movieId","title").where(col("movieId").isin(genre_list))
    msg = "Count: "+str(data.count())
    return data,msg

def get_rec(id):
    pass
"""
get movies for year
"""
def get_year(year_list):
    data = reviews.select("*").where(col("year").isin(year_list))
    msg = "Count: "+str(data.count())
    return data,msg

"""
get movie by name or id
"""
def get_movie(terms):
    ids = []
    names = []
    for t in terms:
        if match("\d+",t): ids.append(int(t))
        else: names.append(t.strip()[1:-1])
        
    data = reviews_avg.where(col("movieId").isin(ids) | col("title").isin(names))
    msg = "Count: "+str(data.count())
    return data,msg

"""
get movies in common between 2 users
"""
def compare(id1, id2):
    id1, id2 = int(id1), int(id2)
    if id1 == id2: return None
    data = getWatched([id1,id2])[0].where(array_contains(col("watchers"),id1))\
        .where(array_contains(col("watchers"),id2))

    common = get_favorite([id1,id2])[0]\
        .groupBy("split").count()\
        .where(1 < col("count"))
    common =  str(list(map(lambda x: x["split"],common.collect())))
    msg = "Count: {} Favorites in common: {}".format(data.count(),common )
    return data,msg

def get_favorite(id_list):
    data = favorites.where(col("userId").isin(id_list))
    msg = "Count: "+str(data.count())
    return data,msg

def handle_command(command_full):
   # try:
        index = command_full.find(" ")
        command = command_full[:index].strip()
        args = command_full[index + 1:].strip()
        if match("^watch ( *\d+ *(,|$))+",command_full):
            return getWatched([int(i) for i in args.split(",")])
            
        elif match("^all *$",command_full):
            return reviews_avg, "Count: "+str(reviews_avg.count())

        elif match("^cluster *$",command_full):
            return clusters, "Count: "+str(clusters.count)

        elif match('^movie ( *("[^"]+"|\d+) *(,|$))+',command_full):
            return get_movie(args.split(","))

        elif match("^comp +\d+ +\d+ *$",command_full):
            id1, id2 = args.split()
            return compare(id1, id2)

        elif match("^genre ( *\w+ *(,|$))+",command_full):
            return get_genre([i for i in args.split(",")])

        elif match("^fav ( *\d+ *(,|$))+",command_full):
            return get_favorite(list(map(int, args.split(","))))

        elif match("^year ( *\d{4} *(,|$))+",command_full):
            return get_year(list(map(int, args.split(","))))
    #except:
    #    return None

def main():
    print("Run main")


    
if __name__ == "__main__":
    main()