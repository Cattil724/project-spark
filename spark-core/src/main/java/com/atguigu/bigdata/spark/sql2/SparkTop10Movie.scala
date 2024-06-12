package com.atguigu.bigdata.spark.sql2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util.Properties


/*
* 对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)。
* */
object SparkTop10Movie {
  def main(args: Array[String]): Unit = {
//    Class.forName("com.mysql.cj.jdbc.Driver")
    val spark:SparkSession=SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark sql shuffle partitions",'4')
      .getOrCreate()
    import spark.implicits._
    //TODO 1.读取电影评分数据，从本地文件系统读取
    val originalRDD: RDD[String] = spark.sparkContext.textFile("data/ml-1m/ratings.dat")
    //println(s"Count=${originalRDD.count()}")
    //println(originalRDD.first())
    //TODO 2.转换数据，指定Schema信息，封装到DataFrame
    val df: DataFrame = originalRDD.map {
      //样本数据:1::1193::5::978300760
      line =>
        val Array(userId, movieId, rating, timestamp) = line.split("::")
        (userId, movieId, rating.toDouble, timestamp.toLong)

    }.toDF("userId", "movieId", "rating", "timestamp")
//    df.printSchema()
//    df.show(10,truncate = false)
    //TODO 3.基于SQL方式分析
    //缓存
    df.persist(StorageLevel.MEMORY_AND_DISK)
    //临时表
      df.createOrReplaceTempView("tmp_view_ratings")
    /*
     *    - 1、按照电影进行分组，计算每个电影平均评分和评分人数
          - 2、过滤获取评分人数大于2000的电影
          - 3、按照电影评分降序排序，再按照评分人数排序
    * */
    val Top10Movie: DataFrame = spark.sql(
      """
        |SELECT
        | movieId,
        | ROUND(AVG(rating),2) AS avg_rating,
        | COUNT(movieId) AS cnt_rating
        |FROM
        | tmp_view_ratings
        | GROUP BY
        | movieId
        | HAVING
        | cnt_rating>=2000
        | ORDER BY
        | avg_rating DESC,cnt_rating DESC
        | LIMIT
        |   10
        |""".stripMargin)
//    Top10Movie.printSchema()
//    Top10Movie.show(10,truncate = false)
    //TODO 4.基于DSL方式分析
    import org.apache.spark.sql.functions._
    val reluDF: Dataset[Row] = df.groupBy($"movieId")
      .agg(
        round(avg($"rating"), 2).as("avg_rating"),
        count($"movieId").as("cnt_rating")
      )
      .where($"cnt_rating">2000)
      .orderBy($"avg_rating".desc, $"cnt_rating".desc)
      .limit(10)
//    reluDF.printSchema()
//    reluDF.show()
    //TODO 保存数据mysql和CSV
    //mysql
//    reluDF
//      .write
//      .mode(SaveMode.Overwrite)
//      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "123456")
//      .jdbc(
//        "jdbc:mysql://node1:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnic ode=true",
//    "atguigu.tb_top10_movies",
//    new Properties()
//    )
    //CSV
    reluDF
      .write
      .mode(SaveMode.Overwrite)
      .option("header","true")
      .csv("data/top10-movies2")
    //释放缓存
    df.unpersist()
    Thread.sleep(1000000)
    spark.stop()
  }


}
