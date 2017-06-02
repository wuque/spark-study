package com.hpsk.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 基于SparkCore实现[网站流量PV和UV]
  */
object TrackLogAnalyzerSpark {

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
        .setAppName("TrackLog Analyzer")
        // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
        // 在实际部署的时候将通过提交应用的命令行进行设置
        .setMaster("local[2]")
    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

    // Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")

/** =============================================================================== */
    /**
      * Step 1: read data
      *     SparkContext用户读取数据
      */
    val trackRdd = sc.textFile("/user/hive/warehouse/db_track.db/yhd_log/date=20150828/")

    // 测试
    println(s"Count = ${trackRdd.count()}" + "\n" + trackRdd.first())

    /**
      * Step 2: process data
      *   RDD#transformation
      */
    /**
      * 需求：
      *     统计每日的PV和UV
      *       PV：页面访问数/浏览量
      *           pv = COUNT(url)   url不能为空 ， url.length > 0    第2列
      *       UV: 访客数
      *           uv = COUNT(DISTINCT guid)                        第6列
      *     时间：
      *        用户访问网站的时间字段
      *        tracktime    -> 2015-08-28 18:10:00                 第18列
      */
    // 清洗数据，提取字段
    val filteredRdd: RDD[(String, String, String)] = trackRdd
      .filter(line => line.trim.length > 0)   // 防止空字符串
      .map(line => {
        // 分割数据
        val splited = line.split("\t")
        // return -> (date, url, guid)  三元组
        (splited(17).substring(0, 10), splited(1), splited(5))
      })

    // RDD数据可以放到内存中（内存空间比较大的话）
    filteredRdd.cache()

    // 统计每日PV
    val pvRdd = filteredRdd
      // 提取所需的字段，此处代码编写风格为case
      .map{
        case (date, url, guid) => (date, url)
      }
      .filter(tuple => tuple._2.trim.length > 0)  // 判断URL不能为空
      // 只要 date 有访问记录表示浏览一次网站
      .map{
        case (date, url) => (date, 1)
      }
      .reduceByKey(_ + _)  // 按照日期进行聚合统计
    // 打印结果RDD中的数据
    pvRdd.foreach(println)
    // (2015-08-28,69197)


    // 统计每日UV
    val uvRdd = filteredRdd
      // 提取所需的字段，此处代码编写风格为case
      .map{
        case (date, url, guid) => (date, guid)
      }
      // 去重，相同guid在某一天如果访问多次的话，仅仅算一次
      .distinct()
      .map{
        case (date, guid) => (date, 1)
      }
      .reduceByKey(_ + _)  // 按照日期进行聚合统计

      // 打印结果RDD中的数据
      // 如果程序运行在Cluster上的时候，下面的打印信息显示在Executor日志中
      uvRdd.foreach(println)
      // (2015-08-28,39007)

    /**
      * RDD Transformations
      */
    val joinRdd: RDD[(String, (Int, Int))] = pvRdd.join(uvRdd)
    joinRdd.foreach{
      case (key, (pvValue, uvValue)) => println("date = " + key + ", PV = " + pvValue + ", UV = " + uvValue)
    }

    val unionRdd: RDD[(String, Int)] = pvRdd.union(uvRdd)
    unionRdd.foreach(println)

    // 将缓存的数据进行释放
    filteredRdd.unpersist()

/** =============================================================================== */

    // 为了WEB UI监控页面
    Thread.sleep(10000000)

    // 关闭资源
    sc.stop()
  }

}
