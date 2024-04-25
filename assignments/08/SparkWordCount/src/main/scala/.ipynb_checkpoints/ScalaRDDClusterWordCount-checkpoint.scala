import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object SparkWordCount { 
  def main(args: Array[String]) {
    // create Spark context with Spark configuration7. 
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val inputFile = args(0)
    val outputFile = args(1)
    val textFile = sc.textFile(inputFile) 
    // Load our input data
    val counts = textFile.flatMap(line => line.split(" ")) // Split up into words
                .map(word => (word, 1)) // Transform into word and count
                .reduceByKey(_ + _) 

    // Save the word count back out to a text file, causing evaluation
    counts.saveAsTextFile(outputFile) 
  }
}
