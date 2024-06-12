package com.atguigu.bigdata.spark.sql2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkRDDInferring {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("SparkRDDInferring").setMaster("local")
    val spark=SparkSession.builder().config(sparkConf).getOrCreate()
    val sc:SparkContext=spark.sparkContext
    import spark.implicits._
    val rawRatingsRDD: RDD[String] = sc.textFile("data/ml-100k/u.data")
    val ratingsRDD: RDD[MovieRating] = rawRatingsRDD.mapPartitions(
      line => line.map { x =>
        val Array(userId, itemId, rating, timestamp) = x.trim.split("\\t")
        MovieRating(userId, itemId, rating.toDouble, timestamp.toLong)
      }
    )
    //直接将RDD[CaseClass]转换成DataFrame
    val ratingsDF:DataFrame=ratingsRDD.toDF()
    ratingsDF.printSchema()
    ratingsDF.show(10)

    val ratingsDS: Dataset[MovieRating] = ratingsRDD.toDS()
    ratingsDS.printSchema()
    ratingsDS.show(10,truncate = false)
    spark.stop()
  }

}
