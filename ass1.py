
from gc import collect
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, array_contains, explode, rank
from curses import wrapper, napms
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("PySpark Movie Database") \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \
sc = spark.sparkContext

MOVIE_DB_DIRECTORY = "ml-latest-small/"

#read the data frames
links = spark.read.csv(MOVIE_DB_DIRECTORY+"links.csv",inferSchema='True',header=True)

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

reviews_avg = reviews.groupBy("movieId","title").avg("rating")#.select("movieId","title")
#reviews_avg.show()

#users and counts of genres they watched
user_genres = reviews.select("userId",explode(reviews.genre_array).alias("split"))\
        .groupBy("userId","split")\
        .count()

def getWatched(id_list):
    return reviews.select("movieId","title","rating").where(reviews.userId.isin(id_list))

def getForGenre(genre_list):
    return reviews.select("movieId","title").where(reviews.movieId.isin(genre_list))

def get_year(year_list):
    return reviews.select("movieId","title").where(reviews.year_list.isin(year_list))

def printDF(df,screen,pad):
    rows, cols = screen.getmaxyx()
    inc = iter(range(pad,rows))
    for row in df.collect():
        count = next(inc,None)
        if count == None:
            break
        screen.addstr(count,4,str(row))

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
    window = Window.partitionBy(user_genres['userId']).orderBy(user_genres['count'].desc())
    user_genres.select('*', rank()
        .over(window)
        .alias('rank')) \
        .filter(col('rank') <= 5) \
        .show()

    
if __name__ == "__main__":
    main(None)
    #wrapper(main)