import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MainOpenFile {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir","C:\\!disk\\Java_Repo\\winutils\\hadoop-2.6.0");
        List<String> inputData = new ArrayList<>();
        inputData.add("WARN: Tuesday 4 September 0405");
        inputData.add("ERROR: Tuesday 4 September 0408");
        inputData.add("FATAL: Wednesday 5 September 1632");
        inputData.add("ERROR: Friday 7 September 1854");
        inputData.add("WARN: Saturday 8 September 1942");

        SparkConf conf = new SparkConf().setAppName("startingspark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        initialRdd
                .flatMap(v -> Arrays.asList(v.split(" ")).iterator())
                .collect()
                .forEach(System.out::println);


        sc.close();

    }
}
