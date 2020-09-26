import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class MainExamResults {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\!disk\\Java_Repo\\winutils\\hadoop-2.6.0");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///C:/tmp/")
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");


        Column co = functions.col("score").cast(DataTypes.IntegerType);
        dataset.groupBy("subject").agg(functions.max(co).alias("max score")).show();


//        dataset.show();/
    }
}
