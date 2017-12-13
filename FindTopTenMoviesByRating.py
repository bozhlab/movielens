
# coding: utf-8

from pyspark import SparkConf, SparkContext
import pyspark
conf = SparkConf()
#set validateOutputSpecs to false to ignore writing file to exists output directory
conf.set("spark.hadoop.validateOutputSpecs", "false")
#sc = SparkContext.getOrCreate()
#sc.stop()
sc = SparkContext(appName = 'FindTopTenMoviesByRating', conf = conf)

# # Rating section

#load ratings data
ratings_raw = sc.textFile("/data/movie-ratings/ml-10M100K/ratings.dat")
#ratings_raw.takeSample(False, 5)

#movie_id, rating
movies_ratings = ratings_raw.map(lambda line: (line.split('::')[1],float((line.split('::')[2]))))
#movies_ratings.takeSample(False, 5)

#get total sum of rating and total number of rating from users seperated by movie_id
sum_count = (0,0)
sum_movies_ratings = movies_ratings.aggregateByKey(sum_count, lambda a,b: (a[0] + b,    a[1] + 1),
                                  lambda a,b: (a[0] + b[0], a[1] + b[1]))
#sum_movies_ratings.takeSample(False, 5)

#get only average rating of each movie
#movie_id, avg_rating
movie_avg_rating = sum_movies_ratings.mapValues(lambda v: round(v[0]/v[1],3)).takeOrdered(10, key = lambda x: -x[1])
#print(movie_avg_rating)


# # Movie section

#load movies data
movies_raw = sc.textFile("/data/movie-ratings/ml-10M100K/movies.dat")
#movies_raw.takeSample(False, 5)

#movie_id, movie_name
movies = movies_raw.map(lambda line: (line.split('::')[0], line.split('::')[1]))
#movies.takeSample(False, 5)

#convert rdd to be dictionary data
movies_list = movies.collect()
movies_list = dict((key, value) for (key,value) in movies_list)

#get the movie name from movies_list and rating from result_movies
top_ten_movies = [(movies_list.get(r[0]),r[1]) for r in movie_avg_rating]
#print(top_ten_movies)

# # Output

#save output to hdfs
sc.parallelize(top_ten_movies).coalesce(1).saveAsTextFile('output/FindTopTenMoviesByRating/')
