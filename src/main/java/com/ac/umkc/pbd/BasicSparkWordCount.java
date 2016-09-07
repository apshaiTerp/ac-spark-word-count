package com.ac.umkc.pbd;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

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
  
  /**
   * @param args list of command line arguments
   */
  @SuppressWarnings("serial")
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println ("You did not specify the input and output file names");
      return;
    }
    
    try {
      SparkConf sparkConfiguration = new SparkConf();
      sparkConfiguration.setAppName("Basic Spark Word Count");
      sparkConfiguration.setMaster("local");
      
      JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);
      JavaRDD<String> messageFile   = sparkContext.textFile(args[0]).cache();
      
      JavaRDD<String> words = messageFile.flatMap(new FlatMapFunction<String, String>() {
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

      counts.saveAsTextFile(args[1]);
      
      sparkContext.close();
      
    } catch (Throwable t) {
      System.err.println ("Something bad happened: " + t.getMessage());
      t.printStackTrace();
    }
  }
}
