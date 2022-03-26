

from gc import collect
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, array_contains, explode, rank, collect_list, avg, count,round
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
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

#all movies
movies = spark.read.csv(MOVIE_DB_DIRECTORY+"movies.csv",inferSchema='True',header=True)\
    .withColumn("genre_array",split(col("genres"),"[|]"))\
    .drop("genres")\
    .withColumn("year",split(col("title"),"[(](?=.....$)|[)]$").getItem(1))
    
    
     #split the generes into a list

ratings = spark.read.csv(MOVIE_DB_DIRECTORY+"ratings.csv",inferSchema='True',header=True)

tags = spark.read.csv(MOVIE_DB_DIRECTORY+"tags.csv",inferSchema='True',header=True)

#join movie and user rating
reviews= ratings.join(movies, 
               ["movieId"], 
               "left"
               )
#average review score
reviews_avg = reviews.groupBy("movieId","title")\
    .agg(
        round(avg(col("rating")),1).alias("avg_rating"),
        count(col("rating")).alias("watches")
    )

#users and counts of genres they watched
user_genres = reviews.select("userId",explode(reviews.genre_array).alias("split"))\
        .groupBy("userId","split")\
        .count()



window = Window.partitionBy(user_genres['userId']).orderBy(user_genres['count'].desc())
favorites = user_genres.select('*', rank()
        .over(window)
        .alias('rank')) \
        .filter(col('rank') <= 5) 


"""
get all movies watched by people in id_list
"""
def getWatched(id_list):
    data = reviews.where(reviews.userId.isin(id_list))\
        .groupBy("movieId","title","rating","genre_array").agg(collect_list('userId').alias('watchers'))\
        .select("watchers","movieId","title",col("genre_array").alias("genres"))
    genre_count = user_genres.where(reviews.userId.isin(id_list)).groupBy("split")\
        .count().count()#total number of genres
    movie_count = data.groupBy("movieId").count().count()
    msg = "Number of movies: {}, Number of genres {}".format(movie_count,genre_count)

    return data, msg

    
            
"""
get all movies in genre
"""
def get_genre(genre_list):
    data = reviews.select("movieId","title").where(reviews.movieId.isin(genre_list))
    msg = "Count: "+str(data.count())
    return data,msg

"""
get movies for year
"""
def get_year(year_list):
    data = reviews.select("*").where(reviews.year_list.isin(year_list))
    msg = "Count: "+str(data.count())
    return data,msg

"""
get
"""
def get_movie(terms):
    ids = [int(t) for t in terms if match("\d+")]
    names = [t.strip() for t in terms if not match("\d+")]

"""
get movies in common between 2 users
"""
def compare(id1, id2):
    id1, id2 = int(id1), int(id2)
    if id1 == id2: return None
    data = getWatched([id1,id2]).where(array_contains(col("watchers"),id1))\
        .where(array_contains(col("watchers"),id2))
    msg = "Count: "+str(data.count())
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
            
        elif match("^movies",command_full):
            return reviews_avg, str(reviews_avg.count())
        elif match("^comp +\d+ +\d+ *$",command_full):
            id1, id2 = args.split()
            return compare(id1, id2)
        elif match("^genre ( *\w+ *(,|$))+",command_full):
            return get_genre([i for i in args.split(",")])
        elif match("^fav ( *\d+ *(,|$))+",command_full):
            return get_favorite(list(map(int, args.split(","))))
    #except:
    #    return None

def main(screen):
    #ftotalrate = ratings.groupBy("userID").sum('rating')

    
    #printDF(getWatched([1,2]),screen,4)
    #input("Test")

    #screen.refresh()
    #napms(10000)


    #movies.where(array_contains(movies.genre_array,"Adventure")).show()
    #select(split(col("genres"),"[|]").alias("NameArray")).show()
    #while True:
    #    id = int(input("Enter ID: "))
    #    things.select("title").where(things.userId == id).show()
    #reviews.show()
    reviews_avg.show()


    
if __name__ == "__main__":
    main(None)
    #wrapper(main)