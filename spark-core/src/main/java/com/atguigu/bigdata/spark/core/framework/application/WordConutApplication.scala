package com.atguigu.bigdata.spark.core.framework.application

import com.atguigu.bigdata.spark.core.framework.common.TApplication
import com.atguigu.bigdata.spark.core.framework.controller.WordConutController

object WordConutApplication extends App with TApplication{

  start(){
    val WordConutController=new WordConutController()
    WordConutController.dispatch()
  }
}
