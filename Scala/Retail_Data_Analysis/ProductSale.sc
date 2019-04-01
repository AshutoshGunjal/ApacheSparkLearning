//Calculate sales breakdown by product category across all of the stores.
package com.df.retail

import scala.math.random

import org.apache.spark.sql.SparkSession
import java.lang.Float


object ProductSales {
	def main(args: Array[String]) {

		if (args.length < 2) {
			System.err.println("Usage: ProductSales <Input-File> <Output-File>");
			System.exit(1);
		}

		val spark = SparkSession
				.builder
				.appName("ProductSales")
				.getOrCreate()

		val data = spark.read.textFile(args(0)).rdd

		val result = data.map { line => {
  		val tokens = line.split("\\t")
		  (tokens(3), Float.parseFloat(tokens(4)))
		}}
//		Prod-Cat  Sale-Val
//		Women's Clothing   153.57
//		Women's Clothing	 483.82
		.reduceByKey(_+_)

		result.saveAsTextFile(args(1))

		spark.stop
	}
}
//bin/spark-submit --class com.df.retail.ProductSales ../retailJob.jar ../Retail_Sample_Data_Set.txt ret-out-01
