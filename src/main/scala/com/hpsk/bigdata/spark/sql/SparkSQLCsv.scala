package com.hpsk.bigdata.spark.sql

import com.hpsk.bigdata.spark.ModuleSpark
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Spark Application 编程模板
  */
object SparkSQLCsv {

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
        .setAppName("SparkSQLCsv")
        // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
        // 在实际部署的时候将通过提交应用的命令行进行设置
        .setMaster("local[2]")
    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

/** =============================================================================== */
    // Create SQLContext
    val sqlContext = new SQLContext(sc)

    /**
      * 读取csv格式的数据
      */
    val ratingsDF: DataFrame = sqlContext
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("G:/ratings.csv")

    // 打印信息
    println(s"Count = ${ratingsDF.count()}")
    // Count = 24404096
    println(ratingsDF.first())
    // [1,122,2.0,945544824]

    // 打印schema 信息
    ratingsDF.printSchema()


    /** =============================================================================== */
    // 关闭资源
    sc.stop()
  }

}
