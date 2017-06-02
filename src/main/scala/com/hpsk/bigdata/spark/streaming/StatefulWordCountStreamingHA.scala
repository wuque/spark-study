package com.hpsk.bigdata.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 从Socket 读取数据，使用SparkStreaming进行实时数据处理
  */
object StatefulWordCountStreamingHA {

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
        .setAppName("WordCountKafkaStreaming")
        // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
        // 在实际部署的时候将通过提交应用的命令行进行设置
        .setMaster("local[3]") // 在本地模式下，启动3个Thread进行运行流式应用
    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")

    val checkpointDirectory = "/datas/sparkstreaming/stateKafka/chkpt999"

    // Function to create and setup a new StreamingContext
    def functionToCreateContext(): StreamingContext = {
      /**
        * 创建SparkStreaming程序的入口，用于读取Streaming Data封装为DStream，
        *     底层按照实时间隔进行划分,编写，查看运行结果设置时间间隔为5s
        */
      val ssc = new StreamingContext(sc, Seconds(5))

      // 处理数据
      processStreamData(ssc)

      // 设置检查点目录, 由于此程序是有状态更新，所以需要将状态信息保存到检查点目录文件中
      ssc.checkpoint(checkpointDirectory)

      println("Creating function called to create new StreamingContext")
      // 返回StreamingContext
      ssc
    }

    // Get StreamingContext from checkpoint data or create a new one
    val context = StreamingContext.getOrCreate(checkpointDirectory,
        functionToCreateContext _)

    // Start the execution of the streams.
    context.start()   // Streaming job running receiver 0
    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
    context.awaitTerminationOrTimeout(5 * 5 * 1000)
  }

  /**
    * 此处才是SparkStreaming读取数据，处理数据以及输出数据
    * @param ssc
    */
  def processStreamData(ssc: StreamingContext): Unit = {
    // 应该是DStream 数据处理及输出
    /**
      * read data from kafka topic
      */
    // Kafka Cluster
    val kafkaParams: Map[String, String] =
    Map("metadata.broker.list" -> "bigdata-training01.hpsk.com:9092")
    // Kafka Topics
    val topics: Set[String] = Set("sparkTopic")
    // Direct Approach From Kafka Topics
    val linesDStream: DStream[String] = KafkaUtils.createDirectStream[
      String, String, StringDecoder, StringDecoder](
      ssc, // StreamingContext
      kafkaParams,
      topics
    ).map(tuple => tuple._2)

    /**
      * processing data
      * DStream#transformation  ->  RDD#transformation
      */
    val pairDStream: DStream[(String, Int)] = linesDStream
      .flatMap(_.split(" "))
      .map((_, 1))

    // updateStateByKey
    val wordCountsDStream = pairDStream.updateStateByKey(
      (values: Seq[Int], state: Option[Int]) => {
        // 获取当前Key传递进来的Value值
        val currentCount = values.sum

        // 获取key以前状态中的值
        val previousCount = state.getOrElse(0)

        // update state and return
        Some(currentCount + previousCount)
      }
    )

    /**
      * output data To console
      */
    wordCountsDStream.print()
  }
}
