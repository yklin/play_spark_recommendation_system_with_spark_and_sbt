
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

object MovieRecommendation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Movie Recommendation")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Read ratings.csv
	val ratingsFile = "data/ratings.csv"
	val dfRatings = spark.read.format("csv")
      .option("header", true)
      .load(ratingsFile)
      .select("userId", "movieId", "rating", "timestamp")
    println("First 10 ratings:")
	dfRatings.show(10)

    // Read movies.csv
	val moviesFile = "data/movies.csv"
	val dfMovies = spark.read.format("csv")
      .option("header", "true")
      .load(moviesFile)
	  .select("movieId", "title", "genres")
    println("First 10 movies:")
    dfMovies.show(10)

    // Check number of ratings, users and movies
    val numRatings = dfRatings.count()
    val numUsers = dfRatings.select("userId").distinct().count()
    val numMovies = dfRatings.select("movieId").distinct().count()
    println("There are " + numRatings + " ratings, " + numUsers + " users and " + numMovies + " movies.")
    println()

    // Create Spark SQL Table, "ratings" and "movies"
	dfRatings.createOrReplaceTempView("ratings")
	dfMovies.createOrReplaceTempView("movies")

    // Using Spark SQL to query the most rated movies, and show the movie title
    val mostRatedMovies = spark.sql("""
      SELECT movies.title, movie_rates.count
      FROM (
        SELECT ratings.movieId, count(distinct userId) as count
        FROM ratings
        GROUP BY ratings.movieId
        ) movie_rates
      JOIN movies on movie_rates.movieId = movies.movieId
      ORDER BY movie_rates.count desc
      """)
    println("Most Rated Movies:")
    mostRatedMovies.show(10)

    // Using Spark SQL to query most active users
    val mostActiveUsers = spark.sql("""
      SELECT ratings.userId, count(*) as count
      FROM ratings 
      GROUP BY ratings.userId
      ORDER BY count desc
      LIMIT 10
      """)
    println("Most Active Users:")
	mostActiveUsers.show(10)


    // Split data set into traning set and test set, and convert them to RDD
    val splits = dfRatings.randomSplit(Array(0.75, 0.25))
	val (trainingData, testData) = (splits(0), splits(1))

	val ratingsRDD = trainingData.rdd.map(row => {
	  val userId = row.getString(0)
	  val movieId = row.getString(1)
	  val ratings = row.getString(2)
	  Rating(userId.toInt, movieId.toInt, ratings.toDouble)
    })

	val testRDD = testData.rdd.map(row => {
      val userId = row.getString(0)
      val movieId = row.getString(1)
      val ratings = row.getString(2)
      Rating(userId.toInt, movieId.toInt, ratings.toDouble)
    })

    // Train ALS model
	val rank = 20
    val numIterations = 15
    val lambda = 0.10
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = false
    val model = new ALS().setIterations(numIterations)
      .setBlocks(block)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank)
      .setSeed(seed)
      .setImplicitPrefs(implicitPrefs)
      .run(ratingsRDD)

    println("Recommended 10 movies for userId: 1")
    println("Rating:(UserID, MovieID, Rating)")
    val top10ForUser = model.recommendProducts(1, 10)
    for (rating <- top10ForUser) {
      println(rating.toString())
    }
  }
}
