package com.hpsk.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Spark Application 编程模板
  */
object MapJoinSpark {

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

    // emp.txt
    val empRDD = sc
      .textFile("D:/emp.txt")
      .map(line => {
        val splited = line.split("\t")
        // (deptno, ename)
        (splited(7), splited(1))
      })

    // dept.txt
    val deptRDD = sc
      .textFile("D:/dept.txt")
      .map(line => {
        val splited = line.split("\t")
        // (deptno, dname)
        (splited(0), splited(1))
      })

    /**
      * EMP 与 DEPT 表数据JOIN操作
      */
    empRDD.join(deptRDD).foreach(println)

    /**
      * 采用广播变量的方式
      */
    // 对小表的数据进行广播
    val broadcastDetpMap: Broadcast[Map[String, String]] = sc.broadcast(deptRDD.collect().toMap)

    // RDD#map
    empRDD.map{
      case (deptno, ename) => (deptno, (ename, broadcastDetpMap.value.get(deptno).get))
    }.foreach(println)

    // 释放
    broadcastDetpMap.unpersist(true)

    Thread.sleep(100000)

    sc.stop()
  }
}