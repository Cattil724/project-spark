package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    //TODO 环境
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("Streaming")
    val ssc=new StreamingContext(sparkConf,Seconds(3))

    //TODO 逻辑语句
    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new myReceiver)
    messageDS.print()


    //开启采集器
    ssc.start()
    //等待采集器关闭
    ssc.awaitTermination()
  }
  /*
  * 自定义采集器
  * 1.继承Receiver 定义泛型 传递参数
  * 2.重新方法
  * */
  class myReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flg=true
    override def onStart(): Unit = {
          new Thread(new Runnable {
            override def run(): Unit = {
              while (flg){
                val message="采集的数据"+new Random().nextInt(10).toString
                store(message)
                Thread.sleep(500)
              }
            }
          }).start()
    }

    override def onStop(): Unit = {
      flg=false
    }
  }

}
