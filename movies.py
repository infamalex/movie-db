

from gc import collect
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import split, when, col, array_contains, explode, rank, collect_list, avg, count,round, array_repeat
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from re import match, findall

spark = SparkSession \
    .builder \
    .appName("PySpark Movie Database") \
    .config("spark.driver.memory", "15g") \
    .getOrCreate()
sc = spark.sparkContext

MOVIE_DB_DIRECTORY = "ml-latest/"

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
    .withColumn("year",split(col("title"),"[(](?=.{5,6}$)|[)] ?$").getItem(1).cast('int'))\
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
#cache table for reuse
CLASS_FILE = "classes.csv"
if exists(MOVIE_DB_DIRECTORY+CLASS_FILE):
    clusters = spark.read.csv(MOVIE_DB_DIRECTORY+CLASS_FILE,inferSchema='True',header=True)\
        .sort("userId",ascending=True)
else:
    pivoted = favorites\
        .where((col("rank") < 6))\
        .select("userId","rank","split",explode(array_repeat("rank",col("rank"))).alias("c"))\
        .groupBy("userId").pivot("split").count()\
        .na.fill(6)

    assemble=VectorAssembler(inputCols=pivoted.columns[1:],outputCol="features")
    assembled_data=assemble.transform(pivoted)

    scale=StandardScaler(inputCol='features',outputCol='standardized')
    data_scale=scale.fit(assembled_data)
    data_scale_output=data_scale.transform(assembled_data)

    kmeans = KMeans(k=16, seed=69)  # 20 clusters
    model = kmeans.fit(data_scale_output)#.select('features'))

    clusters = model.transform(data_scale_output)\
        .select("userId",col("prediction").alias("class"))\
        .sort("userId",ascending=True)
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
    
    query = array_contains(col("genre_array"),genre_list[0])
    for g in genre_list[1:]:
        query |= array_contains(col("genre_array"),g)

    data = reviews_avg.where(query)
    msg = "Count: "+str(data.count())
    return data,msg

"""
get recommended films
"""
def get_rec(id):
    watched = list(map(lambda x: x["movieId"],getWatched(id)[0].collect())) #get watched movies
    favs = list(map(lambda x: x["split"],\
        favorites.where((col("userId") == id)  ).collect() #get top 3 favorite catagories
    ))

    data =  reviews_avg\
        .where(
            (~col("movieId").isin(watched)) & 
            array_contains(col("genre_array"),favs[0]) &
            array_contains(col("genre_array"),favs[1]) &
            array_contains(col("genre_array"),favs[2])
        )\
        .withColumn(
            'score', F.log(10.0,col("watches")) * col("avg_rating"))\
        .sort("score",ascending=False) #find movies from favorite catagories they haven't watched
    msg = "Count: "+str(data.count())
    return data,msg
"""
get movies for year
"""
def get_year(year_list):
    data = reviews_avg.select("*").where(col("year").isin(year_list))
    msg = "Count: "+str(data.count())
    return data,msg

"""
get movie by name or id
"""
def get_movie(terms):
    ids = []
    names = []
    for t in terms:
        if t[0][0] != '"': ids.append(int(t[0]))
        else: names.append(t[0].strip()[1:-1])
        
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
        .where((col("rank") < 4))\
        .groupBy("split").count()\
        .where(1 < col("count"))
    common =  str(list(map(lambda x: x["split"],common.collect())))
    msg = "Count: {} Favorites in common: {}".format(data.count(),common )
    return data,msg

"""
get users favorite genres
"""
def get_favorite(id_list):
    data = favorites.where(col("userId").isin(id_list))
    msg = "Count: "+str(data.count())
    return data,msg

"""
handle user input for queries 
"""
def handle_command(command_full):
    try:
        index = command_full.find(" ")
        command = command_full[:index].strip()
        args = command_full[index + 1:].strip()
        if match(".*, *$",command_full): #check for extra comma at the end of the list 
            return None, "Extra comma"

        elif match("^watch ( *\d+ *(,|$))+",command_full):
            return getWatched([int(i) for i in args.split(",")])
            
        elif match("^all *$",command_full):
            return reviews_avg, "Count: "+str(reviews_avg.count())

        elif match("^cluster *$",command_full):
            return clusters, "Count: "+str(clusters.count())

        elif match('^movie ( *("[^"]+"|\d+) *(,|$))+',command_full):
            return get_movie(findall('("[^"]+"|\d+)(?= *(,|$))',args))

        elif match("^comp +\d+ +\d+ *$",command_full):
            id1, id2 = args.split()
            return compare(id1, id2)

        elif match("^genre ( *\w+ *(,|$))+",command_full):
            return get_genre([i for i in args.split(",")])

        elif match("^fav ( *\d+ *(,|$))+",command_full):
            return get_favorite(list(map(int, args.split(","))))

        elif match("^year ( *\d{4} *(,|$))+",command_full):
            return get_year(list(map(int, args.split(","))))
        elif match("^rec +\d+ *$",command_full):
            return get_rec(int(args))
    except:
        return None, "Unexpected error occurred"

def main():
    print("Run ass1")


    
if __name__ == "__main__":
    main()