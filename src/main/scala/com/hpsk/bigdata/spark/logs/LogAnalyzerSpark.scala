package com.hpsk.bigdata.spark.logs

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LogAnalyzerSpark {

  def main(args: Array[String]): Unit = {

    // Create SparkConf
    val sparkConf = new SparkConf()
      .setAppName("LogAnalyzerSpark")
      // .setMaster("local[2]")
    // Create SparkContext
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")

    // file path
    val logFile = "/datas/access_log"// "d:/apache.access.log"
    // Create RDD From Local FileSystem
    val accessLogsRdd: RDD[ApacheAccessLog] = sc
      // 读取文本数据，一行一行的读取
      .textFile(logFile)
      // 过滤数据
      .filter(ApacheAccessLog.isValidateLogLine)
      // 解析数据
      .map(log => ApacheAccessLog.parseLogLine(log))

    // cache data
    accessLogsRdd.cache()
    println(s"Count : ${accessLogsRdd.count()}")
    println("First: \n" + accessLogsRdd.first())

    /**
      * 需求一：Content Size
      *     The average, min, and max content size of responses returned from the server
      */
    val contentSizeRdd: RDD[Long] = accessLogsRdd.map(_.contentSize)
    // 此RDD使用多次，需要cache
    contentSizeRdd.cache()

    // compute
    val avgContentSize = contentSizeRdd.reduce(_ + _) / contentSizeRdd.count()
    val minContentSize = contentSizeRdd.min()
    val maxContextSize = contentSizeRdd.max()

    contentSizeRdd.unpersist()
    // println
    println(s"Content Size Avg : ${avgContentSize}, Min: ${minContentSize}, Max: ${maxContextSize}")


    /**
      * 需求二：Response Code
      *     A count of response code's returned.
      *
      */
    val responseCodeToCount: Array[(Int, Int)] = accessLogsRdd
      .map(log => (log.responseCode, 1)) // WordCount
      .reduceByKey(_ + _)
      .collect()   // 返回的数组，由于Response Code状态码不多
    println(s"Response Code Count : ${responseCodeToCount.mkString("[", ",", "]")}")

    /**
      * 需求三：IP Address
      *     All IP Addresses that have accessed this server more than N times.
      */
    val ipAddresses: Array[(String, Int)] = accessLogsRdd
      .map(log => (log.ipAddress, 1)) // WordCount
      .reduceByKey(_ + _)
      .filter(_._2 > 20 )
      .take(10)
    println(s"IP Addresses: ${ipAddresses.mkString("[", ",", "]")}")


    /**
      * 需求四：Endpoint
      *     The top endpoints requested by count
      */
    val topEndpoints: Array[(String, Int)] = accessLogsRdd
      .map(log => (log.endpoint, 1)) // WordCount
      .reduceByKey(_ + _)
      .top(5)(OrderingUtils.SecondValueOrdering)
      /*
        .sortBy(tuple => tuple._2, ascending = false)
        .take(5)
      */
    println(s"Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}")

    // 释放空间
    accessLogsRdd.unpersist()

    // WEB UI
    Thread.sleep(100000000)

    // SparkContext
    sc.stop()
  }

}
