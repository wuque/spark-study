package com.hpsk.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Spark Application 编程模板
  */
object WordCountSpark {

  /**
    * Driver Program
    * JVM Process  -  Main 运行的Process
    */
  def main(args: Array[String]): Unit = {
    /**
      * 从前面的spark-shell命令行可知：
      * Spark 数据分析的程序入口为SparkContext，用户读取数据
      */
    // 读取Spark Application的配置信息
    val sparkConf = new SparkConf()
      // 设置Spark Application 名称
      .setAppName("WordCountSpark")
      // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
      // 在实际部署的时候将通过提交应用的命令行进行设置
      .setMaster("local[2]")
    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

    // 读取数据
    val inputRdd = sc.textFile("d:/wc.input")

    // 统计词频时，标点符号不需要进行统计
    // 在Driver端
    val list = List(",", ".", "#", "$", "!", "?", "@")

    /** 未使用广播变量  */
    // RDD API
    val wordCountRdd = inputRdd
      // 分割单词
      .flatMap(_.split(" "))
      // 进行过滤数据，将标点符号过滤
      // filter 属于RDD中每个分区的数据都会进行过滤，在Executor上执行，此处使用list（Driver端）需要拷贝到
      // Executor的每个Task上，存储在内存中，此时消耗内存。
      .filter(word => !list.contains(word))
      // 元组对
      .map((_, 1))
      // 聚合统计
      .reduceByKey(_ + _)

    wordCountRdd.foreach(println)


    // 使用广播变量进行优化

    // 将list广播到Executor中，并将数据存储在Executor内存中，每个Executor仅有一份
    val broadcastList = sc.broadcast(list)

    inputRdd
      // 分割单词
      .flatMap(_.split(" "))
      // 进行过滤数据，将标点符号过滤
      .filter(word => !broadcastList.value.contains(word))
      // 元组对
      .map((_, 1))
      // 聚合统计
      .reduceByKey(_ + _)
      .foreach(println)

    // 当不使用广播变量的值得时候，将广播变量从内存删除
    broadcastList.unpersist(true) // true 表示的是在使用的时候不进行删除，等到没有被使用的时候在进行


    Thread.sleep(100000)

  }
}