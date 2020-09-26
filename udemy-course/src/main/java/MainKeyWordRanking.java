import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import util.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainKeyWordRanking {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\!disk\\Java_Repo\\winutils\\hadoop-2.6.0");

        SparkConf conf = new SparkConf().setAppName("startingspark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        initialRdd.map(s -> s.replaceAll("[^a-zA-Z\\s]","").toLowerCase())
                .filter(s -> s.trim().length() > 0)
                .flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
                .filter(Util::isNotBoring)//Sor out boring words
                .mapToPair(word -> new Tuple2<String, Long>(word,1L))
                .reduceByKey((v1, v2) -> v1 + v2)
                .mapToPair(tuple -> new Tuple2<Long, String>(tuple._2,tuple._1))// Swap number of occurrences with word
                .sortByKey(false)
                .take(50)
                .forEach(System.out::println);


        sc.close();

    }
}
