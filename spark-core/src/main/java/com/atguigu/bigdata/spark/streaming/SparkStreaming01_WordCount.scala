package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc=new StreamingContext(sparkConf,Seconds(3))
    //连接端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    //TODO 逻辑语句
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val mapDS: DStream[(String, Int)] = words.map((_, 1))
    val reduceDS: DStream[(String, Int)] = mapDS.reduceByKey(_ + _)
    reduceDS.print()



    //开启采集器
    ssc.start()
    //等待采集器关闭
    ssc.awaitTermination()
  }

}
