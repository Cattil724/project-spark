package com.atguigu.bigdata.spark.sql2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/*
* 对电影评分数据进行统计分析，获取Top10电影（电影评分平均值最高，并且每个电影被评分的次数大于2000)。
* */
object SparkTop10Movie2 {
  def main(args: Array[String]): Unit = {
    //    Class.forName("com.mysql.cj.jdbc.Driver")
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._
    //TODO 1.读取电影评分数据，从本地文件系统读取
    val dataRDD: RDD[String] = spark.sparkContext.textFile("data/ml-1m/ratings.dat")

    //TODO 2.转换数据，指定Schema信息，封装到DataFrame
    val moveiDF: DataFrame = dataRDD.map {
      line =>
        val Array(userId, movieId, mark, number) = line.split("::")
        (userId, movieId, mark.toDouble, number.toLong)
    }.toDF("userId", "movieId", "mark", "number")
    //样本数据:1::1193::5::978300760
    //    moveiDF.printSchema()
    //    moveiDF.show(10,truncate = false)
    //缓存
    moveiDF.persist(StorageLevel.MEMORY_AND_DISK)
    //TODO 3.基于SQL方式分析

    //临时表
    moveiDF.createOrReplaceTempView("tmp_view_ratings")
    /*
     *    - 1、按照电影进行分组，计算每个电影平均评分和评分人数
          - 2、过滤获取评分人数大于2000的电影
          - 3、按照电影评分降序排序，再按照评分人数排序
    * */
    val movieSQL: DataFrame = spark.sql(
      """
        |SELECT movieId,round(avg(mark),2) AS avg_rating,count(movieId) AS cnt_rating
        |FROM tmp_view_ratings
        |GROUP BY  movieId
        |HAVING cnt_rating>2000
        |ORDER BY avg_rating DESC ,cnt_rating DESC
        |LIMIT 10
        |""".stripMargin)
    //    movieSQL.printSchema()
    //    movieSQL.show(10,truncate = false)
    //TODO 4.基于DSL方式分析
    import org.apache.spark.sql.functions._
    val movieDSL: Dataset[Row] = moveiDF.groupBy($"movieId").agg(
      round(avg($"mark"), 2).as("avg_rating"),
      count($"movieId").as("cnt_rating")
    )
      .where($"cnt_rating" > 2000)
      .orderBy($"avg_rating".desc, $"cnt_rating".desc)
    movieDSL.printSchema()
    movieDSL.show(10, truncate = false)
    //TODO 保存数据mysql和CSV
    //mysql
    //CSV
    movieSQL.write.mode(SaveMode.Overwrite).option("header", "true").csv("data/top10-movies")
    //释放缓存
    moveiDF.unpersist()
    Thread.sleep(1000000)
    spark.stop()
  }


}
