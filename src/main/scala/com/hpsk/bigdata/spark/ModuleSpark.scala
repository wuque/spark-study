package com.hpsk.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

class ModuleSpark

/**
  * Spark Application 编程模板
  */
object ModuleSpark {

  /**
    * Driver Program
    *     JVM Process  -  Main 运行的Process
    */
  def main(args: Array[String]): Unit = {
    /**
      * 从前面的spark-shell命令行可知：
      *     Spark 数据分析的程序入口为SparkContext，用户读取数据
      */
    // 读取Spark Application的配置信息
    val sparkConf = new SparkConf()
        // 设置Spark Application 名称
        .setAppName(classOf[ModuleSpark].getSimpleName)
        // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
        // 在实际部署的时候将通过提交应用的命令行进行设置
        .setMaster("local[2]")
    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

/** =============================================================================== */
    /**
      * Step 1: read data
      *     SparkContext用户读取数据
      */

    /**
      * Step 2: process data
      *   RDD#transformation
      */

    /**
      * Step 3: write data
      *     将处理的结果数据储存
      *     RDD#action
      */


/** =============================================================================== */
    // 关闭资源
    sc.stop()
  }

}
