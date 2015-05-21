# scala-sbt
Scala programs to run on Spark 

# prerequisite
sbt  2.10.4
spark 1.1.1 or higher
scala 2.10.4

##compile
sbt package

##run
Navigate to Spark 
./bin/spark-submit --master local --class "WordsMinDistance" /path/to/jar /path/to/dir(file) "search words"
