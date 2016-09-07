package com.ac.umkc.pbd;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;


/**
 * The goal of this program is to take a file of curated tweets by the venerable Donald Trump
 * (@realDonaldTrump) which has been previously uploaded into HDFS, and run it through a word
 * count utility to see which words are used most often.
 * 
 * Technologies used here are:
 * <ul><li>Apache Spark libraries for Map/Reduce simplicity</li>
 * <li>Scala is imported to give us Tuples to use</li>
 * <li>Maven is used as the build utility</li></ul>
 * 
 * This program is a modified adaptation of the Spark Tutorial Examples provided
 * <a href="https://github.com/apache/spark/blob/master/examples/src/main/java/org/apache/spark/examples/JavaWordCount.java">here</a>
 * 
 * @author AC010168
 *
 */
public class BasicSparkWordCount {
  
  /** A simple regex pattern for identifying spaces */
  private static final Pattern SPACE = Pattern.compile(" ");
  
  /** Internal reference to our file path in HDFS passed from command line */
  private String hdfsFile;

  public BasicSparkWordCount(String hdfsFile) {
    this.hdfsFile = hdfsFile;
  }
  
  /**
   * @param args list of command line arguments
   */
  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println ("No file path provided as $1 input");
      return;
    }
    
    BasicSparkWordCount count = new BasicSparkWordCount(args[0]);
    count.execute();
  }
  
  /**
   * Helper method to drive the program execution.
   */
  @SuppressWarnings("serial")
  public void execute() {
    try {
      SparkSession spark = SparkSession.builder().appName("JavaWordCount").getOrCreate();
      JavaRDD<String> lines = spark.read().textFile(hdfsFile).javaRDD();

      JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
          public Iterator<String> call(String s) {
            return Arrays.asList(SPACE.split(s)).iterator();
          }
      });

      JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
      });

      JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
      });

      List<Tuple2<String, Integer>> output = counts.collect();
      for (Tuple2<?,?> tuple : output) {
        System.out.println(tuple._1() + ": " + tuple._2());
      }
      
      spark.stop();      
      
    } catch (Throwable t) {
      System.err.println ("Something bad happened: " + t.getMessage());
      t.printStackTrace();
    }
  }
}