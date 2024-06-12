package com.atguigu.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

object EnvUtil {
    private val sclcal=new ThreadLocal[SparkContext]()
    def put(sc:SparkContext){
    sclcal.set(sc)
  }
  def take(): SparkContext ={
    sclcal.get()
  }

  def clear(): Unit ={
    sclcal.remove()
  }
}
