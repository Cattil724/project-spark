package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkconf=new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark=SparkSession.builder().enableHiveSupport().config(sparkconf).getOrCreate()
    //使用sparksql连接外置的hive
    //1.拷贝hive-size.xml文件到classpath下
    //2.启用hive的支持
    //3.增加对应的依赖关系（包含mysql驱动）
    spark.sql("show databases").show()

    //TODO 关闭环境
    spark.close()
  }

}
