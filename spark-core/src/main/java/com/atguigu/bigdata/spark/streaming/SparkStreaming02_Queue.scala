package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_Queue {
  def main(args: Array[String]): Unit = {
    //TODO 环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc=new StreamingContext(sparkConf,Seconds(3))

    //TODO 逻辑语句
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)
    val mappedStream = inputStream.map((_,1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()


    //开启采集器
    ssc.start()
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    //等待采集器关闭
    ssc.awaitTermination()
  }

}
