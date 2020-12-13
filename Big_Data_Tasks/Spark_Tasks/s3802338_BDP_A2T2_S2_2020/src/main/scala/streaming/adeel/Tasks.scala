package streaming.adeel

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.text.SimpleDateFormat;
import java.util.Date;

object Tasks {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }
 
    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("Spark Task Assignment 2").setMaster("local")
    
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    
    // Create the FileInputDStream on the directory and use the stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    
    // Remove these punctuation marks
    val remPuncMarks = "[,;:.\'\"!?\\[\\{\\(\\<\\]\\)\\}\\>]"
    
    //iterate through RDD
    lines.foreachRDD{ line =>
      
       //time stamp for creating different folder to avoid file over writing
       val timeStamp = new SimpleDateFormat("hh-mm-ss").format(new Date())
      
       // Split on space and replace punctuation marks and convert all words to Lower case
       val words = line.flatMap(_.split("\\s+")).map(_.replaceAll(remPuncMarks, "").toLowerCase.trim).filter(!_.isEmpty)
       
       // Task 1 word count
       val task1 = words.map((_, 1)).reduceByKey(_ + _)
       
      //Saving rdd to output directory for task 1
       if (!task1.isEmpty())
         task1.coalesce(1).saveAsTextFile(args(1) + timeStamp)
             
       // Task 2 filter words having length less than 5
       val task2 = words.filter(wrd => wrd.length < 5)
       
       //Saving rdd to output directory for task 2
       if (!task2.isEmpty())
         task2.coalesce(1).saveAsTextFile(args(2) + timeStamp) 
       
       //Task 3 count the cooccurance of words
       val task3 = line.map(_.replaceAll(remPuncMarks, "")
           .trim.toLowerCase).filter(!_.isEmpty).map(_.split("\\s+").zipWithIndex.combinations(2).toList)
           .map(_.groupBy(wrd=>(wrd(0)._1, wrd(1)._1))
           .mapValues(_.size).toList)
             
       //Saving rdd for task 3 to output directory from command line argument 4
       if (!task3.isEmpty())
         task3.coalesce(1).saveAsTextFile(args(3) + timeStamp)
    }
  
    ssc.start()
    ssc.awaitTermination()
  }
}