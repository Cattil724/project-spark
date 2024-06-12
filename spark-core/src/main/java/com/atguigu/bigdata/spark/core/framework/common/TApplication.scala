package com.atguigu.bigdata.spark.core.framework.common

import com.atguigu.bigdata.spark.core.framework.controller.WordConutController
import com.atguigu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}


trait TApplication {

      def start(master:String="local[*]",app:String="Application")( op: => Unit): Unit ={

        val conf=new SparkConf().setMaster("local[*]").setAppName("WordCount")
        val  sc=new SparkContext(conf)
        EnvUtil.put(sc)
        try {
          op
        }catch {
          case ex=>println(ex.getMessage)
        }
        sc.stop()
        EnvUtil.clear()
      }
}
