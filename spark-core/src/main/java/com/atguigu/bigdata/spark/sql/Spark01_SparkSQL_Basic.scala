package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO 创建SparkSQL的运行环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkSQl")
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //TODO 执行逻辑代码
    //DataFrame
    //val df: DataFrame = spark.read.json("data/user.json")
//    df.show()
    //DataFrame => SQL
    //df.createOrReplaceTempView("user")

//    spark.sql("select * from user").show()
//    spark.sql("select age from user").show()
//    spark.sql("select avg(age) from user").show()

    //DataFrame => DSL

    //df.select("age","name").show()
    //df.select($"age"+1).show()
    //df.select('age+2).show()

    //TODO DataSet
    //DataFrame其实是特质泛型的DataSet
    //val seq=Seq(1,2,3,4)
    //val ds: Dataset[Int] = seq.toDS()
    //ds.show()
//    ds.select()
    //RDD <=> DataFrame
    val rdd=spark.sparkContext.makeRDD(List((1,"zhang",30),(2,"lishi",30)))
    val df: DataFrame = rdd.toDF("id", "name", "age")

    val rdd1: RDD[Row] = df.rdd

    //DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]

    val df1: DataFrame = df.toDF()

    //RDD <=> DataSet
    val userRDD: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    val rdd2: RDD[User] = ds.rdd
    //TODO 关闭环境
    spark.close()
  }
  case class User(id:Int,name:String,age:Int)

}
