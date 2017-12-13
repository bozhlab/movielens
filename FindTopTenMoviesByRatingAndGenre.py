
# coding: utf-8

from pyspark import SparkConf, SparkContext
import pyspark
conf = SparkConf()
#set validateOutputSpecs to false to ignore writing file to exists output directory
conf.set("spark.hadoop.validateOutputSpecs", "false")
#sc = SparkContext.getOrCreate()
#sc.stop()
sc = SparkContext(appName = 'FindTopTenMoviesByRatingAndGenre', conf = conf)

# # Genre section

#load movies data
movies_raw = sc.textFile("/data/movie-ratings/ml-10M100K/movies.dat")
#movies_raw.takeSample(False, 5)

genres = movies_raw.flatMap(lambda line: line.split('::')[2].split('|')).distinct()
#genres.takeSample(False, 5)

total_genres = genres.collect()
#print(genres.collect())

# # Rating section

#load ratings data
ratings_raw = sc.textFile("/data/movie-ratings/ml-10M100K/ratings.dat")
#ratings_raw.takeSample(False, 5)

#movie_id, rating
movies_ratings = ratings_raw.map(lambda line: (line.split('::')[1],float((line.split('::')[2]))))
#movies_ratings.takeSample(False, 5)

rdd = {}
for g in total_genres:
    #get only movie_id in that genre
    movies_list = movies_raw.filter(lambda line: g in line.split('::')[2]).map(lambda line: (line.split('::')[0])).collect()

    #get only rating from those movies
    movies_ratings = movies_ratings.filter(lambda mv: mv[0] in movies_list)

    #get total sum of rating and total number of rating from users seperated by movie_id
    sum_count = (0,0)
    sum_movies_ratings = movies_ratings.aggregateByKey(sum_count, lambda a,b: (a[0] + b,    a[1] + 1),
                                  lambda a,b: (a[0] + b[0], a[1] + b[1]))

    movie_avg_rating = sum_movies_ratings.mapValues(lambda v: round(v[0]/v[1],3)).takeOrdered(10, key = lambda x: -x[1])

    #find movie_name
    movies = movies_raw.map(lambda line: (line.split('::')[0], line.split('::')[1]))
    movies_list = movies.collect()
    movies_list = dict((key, value) for (key,value) in movies_list)
    top_ten_movies = [(movies_list.get(r[0]),r[1]) for r in movie_avg_rating]

    rdd[g] = top_ten_movies

#print(rdd)

# # Output

#save output to hdfs
sc.parallelize(rdd.items()).coalesce(1).saveAsTextFile('output/FindTopTenMoviesByRatingAndGenre/')
