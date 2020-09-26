import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class MainExercise
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir","C:\\!disk\\Java_Repo\\winutils\\hadoop-2.6.0");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		// TODO - over to you!



		JavaPairRDD<Integer, Integer> chapterCount = chapterData
				.mapToPair(row  -> new Tuple2<Integer, Integer>(row._2,1))
				.reduceByKey(Integer::sum);


//		chapterCount.collect().forEach(System.out::println);

		JavaPairRDD<Integer, Tuple2<Long,Integer>> userIDAndViews =
				viewData.distinct()
				.mapToPair(r -> new Tuple2<Integer, Integer>(r._2, r._1))
				.join(chapterData)
						.mapToPair(r-> new Tuple2<>(r._2,1L))
						.reduceByKey(Long::sum)
						.mapToPair(r-> new Tuple2<>(r._1._2,r._2))
						.join(chapterCount);
						//Course , (number of views , out Of X chapters)
						//		(1,(1,3))
						userIDAndViews.collect().forEach(System.out::println);

		//(Course, percentage viewed)
		//(1,0.3333333333333333)
		//Somebody watched course  0,333 of course 1
		JavaPairRDD<Integer, Double> percentages = userIDAndViews.mapValues(value -> (double)value._1/value._2);
		percentages.collect()
				.forEach(System.out::println);

		//Convert to scores
		JavaPairRDD<Integer,Long> scores = percentages.mapValues(v1 -> {
			if(v1 >0.9 ) return 10L ;
			if(v1 >0.5 ) return 4L ;
			if(v1 >0.25 ) return 2L ;
			return 0L;	});

		scores.collect()
				.forEach(System.out::println);

		scores.reduceByKey(Long::sum).collect().forEach(System.out::println);

		// Total score
//						.collect().forEach(System.out::println);
//				.mapToPair(r -> r._2) //userid , number of views
//				.mapToPair(r->new Tuple2<>(r._2,r._1)); // number of views , userid
//
//
//				userIDAndViews.collect().forEach(System.out::println);


//		JavaPairRDD<Integer, Double> chapterToPercentage = chapterData
//				.mapToPair(r->new Tuple2<>(r._2,1L))
//				.reduceByKey(Long::sum)
//				.mapToPair(r -> new Tuple2<Integer, Double>(r._1, 1.00 / (double) r._2));
//
//		chapterToPercentage
//				.collect()
//				.forEach(System.out::println);
//
//		userIDAndViews.join(chapterToPercentage)//.collect().forEach(System.out::println);
//			.mapToPair(r -> r._2).collect().forEach(System.out::println);
//				.reduceByKey(Double::sum).collect().forEach(System.out::println);


		sc.close();
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
