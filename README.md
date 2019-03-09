## Build and Run

``` sh
$ sbt package
$ spark-submit --class "MovieRecommendation" \
               --master local[4] \
               --driver-memory 4g \
               target/scala-2.11/movie-recommendation_2.11-1.0.jar 
```
