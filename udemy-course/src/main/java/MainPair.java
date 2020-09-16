import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple2$mcCC$sp;

import java.util.ArrayList;
import java.util.List;

public class MainPair {
    public static void main(String[] args) {
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf().setAppName("startingspark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> originalMessages = sc.parallelize(inputData);

//        JavaPairRDD<String , String > pairRdd = originalMessages.mapToPair(rawValue -> {
//            String[] columns = rawValue.split(":");
//            String level = columns[0];
//            String date = columns[1];
//
//            return new Tuple2<>( level, date);
//
//        });



//
//        JavaPairRDD<String , Long > pairRdd = originalMessages.mapToPair(rawValue -> {
//            String[] columns = rawValue.split(":");
//            String level = columns[0];
//            String date = columns[1];
//
//            return new Tuple2<>( level, 1L);
//        });
//
//        JavaPairRDD<String,Long> sumsRdd = pairRdd.reduceByKey((v1, v2) -> v1+v2);
//
//        sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has "+ tuple._2 + " instances ") );

        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ))
                .reduceByKey((v1, v2) -> v1+v2)
                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        //group by key version  - suffers from performance
        sc.parallelize(inputData)
                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L))
                .groupByKey()
                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances "));


        sc.close();

    }
}
