package com.hpsk.bigdata.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 从Socket 读取数据，使用SparkStreaming进行实时数据处理
  */
object WordCountStreaming {

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
        .setAppName("WordCountStreaming")
        // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
        // 在实际部署的时候将通过提交应用的命令行进行设置
        .setMaster("local[3]") // 在本地模式下，启动3个Thread进行运行流式应用
    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel("WARN")

    /**
      * 创建SparkStreaming程序的入口，用于读取Streaming Data封装为DStream，
      *     底层按照实时间隔进行划分,编写，查看运行结果设置时间间隔为5s
      */
    val ssc = new StreamingContext(sc, Seconds(5))
/** =============================================================================== */
    /**
      * read data from network
      *   Create a DStream that will connect to host:port, like localhost:9999
      */
    val linesDStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata-training01.hpsk.com", 9999)

    /**
      * processing data
      *     DStream#transformation  ->  RDD#transformation
      */
    // Split each line into words
    val wordsDStream: DStream[String] = linesDStream.flatMap(_.split(" "))

    // Count each word in each batch
    val pairsDStream = wordsDStream.map((_, 1))
    val wordCountsDStream: DStream[(String, Int)] = pairsDStream.reduceByKey(_ + _)

    /**
      * output data To console
      *   Print the first ten elements for each RDD generated in this DStream to the console
      */
    wordCountsDStream.print()

    /**
      * DStream Save
      */
    wordCountsDStream.saveAsTextFiles(
        prefix = System.currentTimeMillis() + "", suffix = ".result")

    /**
      * DStream#foreachRDD
      *     -1，将数据保存到Redis、HBase、RDBMs中，使用此方法
      *
      *     -2, Spark Integration
      *         与SparkSQL集成分析数据
      */
    wordCountsDStream.foreachRDD((rdd, batchTime) => {
      // TODO: 使用rdd的时候，注意缓存
      rdd.cache()

      /**
        * 操作一、可以将RDD保存到HDFS文件
        */
      rdd.saveAsTextFile(System.currentTimeMillis() + "-" + batchTime + ".result")

      /**
        * 操作二、数据保存到数据库中
        */
      rdd.coalesce(1).foreachPartition(iter => {
        // step 1: Connection pool
        // TODO

        // step 2: Partition Data
        iter.foreach(item => {
          // Insert Data To
        })

        // step 3: return to the pool for future reuse
        // TODO
      })

      /**
        * 操作三：集成SparkCore、SQL
        */
      /**
        * TODO: 集成SQL操作
        *   RDD -> DataFrame
        */
      // SQL/DSL

      // TODO: rdd使用结束以后，注意释放
      rdd.unpersist()
    })

    /**
      * 注意，对于SparkStreaming流式应用来说，
      *     第一点：需要启动应用（不是说运行程序）
      *     第二点：流式应用处理，只要程序运行起来以后，一直在运行，除非人为停止程序运行或者程序异常终止
      */
    // Start the execution of the streams.
    ssc.start()   // Streaming job running receiver 0
    // Wait for the execution to stop.
    ssc.awaitTermination()

    /** =============================================================================== */
    // 关闭资源
    ssc.stop()
  }

}
