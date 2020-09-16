import com.google.inject.internal.cglib.proxy.$FixedValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<Integer>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        SparkConf conf = new SparkConf().setAppName("startingspark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> myRdd = sc.parallelize(inputData);
//
//        Integer result = myRdd.reduce((value1 , value2 ) -> value1+ value2);

        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = myRdd.map(value -> new Tuple2<Integer, Double>(value, Math.sqrt(value)));

//        JavaRDD<Double> sqrtRdd =  myRdd.map(Math::sqrt);



//        sqrtRdd.collect().forEach(System.out::println);


        Tuple2<Integer, Double> myValue = new Tuple2<>(9,3.0);

//        //How many elements in sqrt
//        //using map and reduce
//        JavaRDD<Long> singleRdd = sqrtRdd.map(value -> 1L);
//        Long count = singleRdd.reduce((value1, value2) -> value1 + value2);
//        System.out.println(count);




        sc.close();


    }
}
