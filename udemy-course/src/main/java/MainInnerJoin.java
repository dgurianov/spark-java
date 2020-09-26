import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainInnerJoin {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\!disk\\Java_Repo\\winutils\\hadoop-2.6.0");

        SparkConf conf = new SparkConf().setAppName("startingspark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //Userid , Visits
        List<Tuple2<Integer, Integer>> visitstRaw = new ArrayList<>();
        visitstRaw.add(new Tuple2<>(4,18));
        visitstRaw.add(new Tuple2<>(6,4));
        visitstRaw.add(new Tuple2<>(10,9));

        //Userid , Name
        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Mary"));
        usersRaw.add(new Tuple2<>(6, "Raq"));

        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitstRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        //(Type from left of visits RDD ,(type from right of visits RDD  , type from right from users RDD))
        //(userID , (visits , name))
        //(4,(18,Doris))
        //(6,(4,Raq))
        JavaPairRDD<Integer, Tuple2<Integer, String>> joined = visits.join(users);
        joined.collect().forEach(System.out::println);


        sc.close();

    }
}
