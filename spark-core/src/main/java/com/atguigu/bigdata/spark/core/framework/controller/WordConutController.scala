package com.atguigu.bigdata.spark.core.framework.controller


import com.atguigu.bigdata.spark.core.framework.common. TController
import com.atguigu.bigdata.spark.core.framework.service.WordConutService
/*
* 调度层
* */
class WordConutController extends TController{
    private val WordConutService=new WordConutService()

    def dispatch():Unit= {
        val array=WordConutService.dataAnalysis()
      array.foreach(println)
    }
}
