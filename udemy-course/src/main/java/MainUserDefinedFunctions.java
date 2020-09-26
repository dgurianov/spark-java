import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class MainUserDefinedFunctions {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\!disk\\Java_Repo\\winutils\\hadoop-2.6.0");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///C:/tmp/")
                .getOrCreate();

//        spark.udf().register("hasPassed",(String grade) -> grade.equals("A+"),DataTypes.BooleanType);
        spark.udf().register("hasPassed",(String grade, String subject) ->{
            if(subject.equals("Biology")){
                if(grade.startsWith("A")) return  true;
                return false ;
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);


        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

//        dataset.withColumn("pass", functions.lit("YES")).show();
//        dataset.withColumn("pass", functions.lit(functions.col("grade").equalTo("A+"))).show();
        dataset.withColumn("pass", functions.callUDF("hasPassed",functions.col("grade"))).show();

//        dataset.show();
    }
}
