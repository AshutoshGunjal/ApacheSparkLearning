//2. Most populous Countries - List of top 10 countries in the descending order of their population
package com.dataflair.wb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long

object PopulousCountry {
	def main(args: Array[String]) = {

   		if (args.length < 2) {
        System.err.println("Usage: JavaWordCount <Input-File> <Output-File>");
        System.exit(1);
      }

			val spark = SparkSession
				.builder
				.appName("AvgAnsTime")
				.master("local")
				.getOrCreate()
				
			val data = spark.read.csv(args(0)).rdd
			
			val result = data.map { line => {
			  val population = line.getString(9).replaceAll(",", "")
			  var popNum = 0L
			  if (population.length() > 0)
			    popNum = Long.parseLong(population)
			  
			  (line.getString(0), popNum)
			}}
			.groupByKey()
//			India  [3018294, 45304958, 7238472, 102938102.....]
			.map(rec => {
			  (rec._1, rec._2.max)
			})
//			India    1298723948723
			.sortBy(rec => (rec._2), false)
			.take(10)
			
			spark.sparkContext.parallelize(result.toSeq, 1).saveAsTextFile(args(1))
			
			spark.stop
	}
}
//bin/spark-submit --class com.dataflairf.wb.PopulousCountry ../wbJob.jar ../World_Bank_Indicators.csv wb-out-002
