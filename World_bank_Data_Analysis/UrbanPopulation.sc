//1.Which country has the highest urban population
package com.dataflair.wb

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.lang.Long

object UrbanPopulation {
	def main(args: Array[String]) = {

   		if (args.length < 2) {
        System.err.println("Usage: UrbanPopulation <Input-File> <Output-File>");
        System.exit(1);
      }

			val spark = SparkSession
				.builder
				.appName("UrbanPopulation")
				.getOrCreate()
				
			val data = spark.read.csv(args(0)).rdd
			
			val result = data.map { line => {
			  val uPopulation = line.getString(10).replaceAll(",", "")
			  var uPopNum = 0L
			  if (uPopulation.length() > 0 || uPopulation.isEmpty())
			    uPopNum = Long.parseLong(uPopulation)
			  
			  (uPopNum, line.getString(0)) //Pair RDD
			}}
			.sortByKey(false)
			.first

			spark.sparkContext.parallelize(Seq(result)).saveAsTextFile(args(1))
			
			spark.stop
	}
}
//bin/spark-submit --class com.dataflair.wb.UrbanPopulation ../wbJob.jar ../World_Bank_Indicators.csv wb-out-002
