package sql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static  org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","C:\\!disk\\Java_Repo\\winutils\\hadoop-2.6.0");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder()
                .appName("testingSql")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///C:/tmp/")
                .getOrCreate();
        Dataset<Row> dataset = spark.read().option("header",true).csv("src/main/resources/exams/students.csv");
//        dataset.show(); // Display data

        Row firstRow = dataset.first(); // First row
        String subject = firstRow.get(2).toString(); // Value from raw
        String subject1 = firstRow.getAs("subject").toString(); // Value by name of column
        int year = Integer.parseInt(firstRow.getAs("year")); // Value by name of column

        //Filter using expression
        Dataset<Row> modernArtResults =  dataset.filter("subject = 'Modern Art AND year >= 2007'");
//        modernArtResults.show();

        //Filter using lambdas
        Dataset<Row> modernArtResultsLambda = dataset.filter(
                row -> row.getAs("subject")
                          .equals("Modern Art")
                          &&
                          Integer.parseInt(row.getAs("year")) >= 2007);
//        modernArtResultsLambda.show();

        //Filter using columns
        Column subjectColumn = col("subject");
        Column yearColumn = col("year");

        Dataset<Row> modernArtResultsColumn = dataset.filter(subjectColumn.equalTo("Modern Art")
                                                                            .and(yearColumn.geq(2007)));
//        modernArtResultsColumn.show();

        //Temporary View
        dataset.createOrReplaceTempView("my_students_table");
        Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
//        results.show();

        //In memory data
        List inMemory = new ArrayList<Row>();
        inMemory.add(RowFactory.create("WARN", "2018-12-16 04:19:32"));
        inMemory.add(RowFactory.create("ERROR", "2018-12-16 04:20:32"));
        inMemory.add(RowFactory.create("INFO", "2018-12-16 04:22:32"));
        inMemory.add(RowFactory.create("WARN", "2018-12-16 04:44:32"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> datasetInmemory = spark.createDataFrame(inMemory,schema);
//        datasetInmemory.show();


        //Grouping and aggregation
        datasetInmemory.createOrReplaceTempView("logging_table");
        Dataset<Row> resultsGrouping = spark.sql("select level, count(datetime) from logging_table group by level order by level");
//        resultsGrouping.show();

        Dataset<Row> resultsGrouping2 = spark.sql("select level , date_format(datetime, 'MMMM') as month from logging_table");
//        resultsGrouping2.show();


        //Data Frames
        datasetInmemory.select("level","datetime");//.show();
        datasetInmemory.selectExpr("level","date_format(datetime,'MMMM')");//.show();
        datasetInmemory.select(col("level"),date_format(col("datetime"),"MMMM").alias("month"));
//        datasetInmemory.groupBy(col("level"),col("month")).count();
        datasetInmemory.drop(col("level"));

        //Pivot table
        //Level , Month , total
        //INFO , APRIL , 1342
        //FATAL, DECEMBER , 45
        //FATAL, MARCH , 111
        //WARN, MARCH , 103

        // became

        //       APRIL, MARCH , DECEMBER
        //INFO    1342   0        0
        //FATAL    0    111        45
        //WARN    0    103       0
//
        Dataset<Row> datasetPivotRaw = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
        Dataset<Row> datasetPivot = datasetPivotRaw.select(col("level"),
                date_format(col("datetime"),"MMMM").alias("month"),
                date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));

        Object[] months = new Object[]{"January","February","March","April","May","June","July","August","September","October","November","December"};
        List<Object> columns = Arrays.asList(months);

        datasetPivot.groupBy("level").pivot("month", columns).count()
                .na() // If no data in cell
                .fill(0)//Fill with 0
                .show();


        spark.close();



    }
}
