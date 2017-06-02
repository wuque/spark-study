package com.hpsk.bigdata.spark.logs

import com.hpsk.bigdata.spark.sql.MaxSalUDAF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 基于SparkSQL进行日志数据分析
  *     - SQLContext 读取数据
  *       val sqlConext: SparkContext
  *     - DataFrame
  *       API：DSL
  *       SQL：HiveQL
  */
object SQLLogAnalyzerSpark {

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
      .setAppName("SQLLogAnalyzerSpark")
      // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
      // 在实际部署的时候将通过提交应用的命令行进行设置
      .setMaster("local[2]")

    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("WARN")

    /**
      * 创建SQLContext上下文，读取数据
      * SparkSQL程序的入口
      */
    val sqlContext = SQLContext.getOrCreate(sc) // new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    /**
      * 一般情况下，在创建SQLContext实例以后，就进行自定义函数定义注册，以便后续编程进行使用
      */
    // 此时没有写明函数返回类型呢？由于SCALA语言可以自动推断
    sqlContext.udf.register(
      "toLower", // function name
      (word: String) => word.toLowerCase
    )


    // 注册UDAF
    sqlContext.udf.register(
      "sel_max",
      MaxSalUDAF
    )

    /** ===============================================================================*/
    /**
      * 由于要处理的数据格式为txt，需要转换为DataFrame，
      *      RDD -> DataFrame
      *           加上 schema信息
      *      DataFrame: 相当于数据库的一张表：字段名称、类型、值 -> 一行一行的数据
      */
    // file path
    val logFile = "d:/access_log"
    // Create RDD From Local FileSystem
    val accessLogsRdd: RDD[ApacheAccessLog] = sc
      // 读取文本数据，一行一行的读取
      .textFile(logFile)
      // 过滤数据
      .filter(ApacheAccessLog.isValidateLogLine)
      // 解析数据
      .map(log => ApacheAccessLog.parseLogLine(log))

    // Create DataFrame
    // RDD[CASE CLASS] -> toDF() 转换为DataFrame, 此种方式属于schema 自动的依据CASE CLASS的属性推断
    val accessLogsDF: DataFrame = accessLogsRdd.toDF()   // 隐式转换

    // 查看DataFrame中schema信息
    /**
      * root
         |-- ipAddress: string (nullable = true)
         |-- clientIdented: string (nullable = true)
         |-- userId: string (nullable = true)
         |-- dateTime: string (nullable = true)
         |-- method: string (nullable = true)
         |-- endpoint: string (nullable = true)
         |-- protocol: string (nullable = true)
         |-- responseCode: integer (nullable = false)
         |-- contentSize: long (nullable = false)
      */
    // accessLogsDF.printSchema()
    // 样本数据
    // accessLogsDF.show(5)

    /**
      * 采用SQL方式进行数据分析
      */
    // 需要将DataFrame注册为一张临时表，表的名称要符合Hive 中表的名称定义规范
    accessLogsDF.registerTempTable("tmp_access_log")  // 相当于Hive中一张表
    // 由于临时表的数据将被使用多次，所以将表的数据放到内存中
    sqlContext.cacheTable("tmp_access_log")

    /**
      * 需求一：Content Size
      *     The average, min, and max content size of responses returned from the server
      *
      * DataFrame里面的数据都是Row，Row表示的是DataFrame中的每一行数据，有多个字段，将ROW当做一个数组
      *
      */
    val contentSizeRow: Row = sqlContext.sql(
      """
        |SELECT
        | SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize)
        |FROM
        | tmp_access_log
      """.stripMargin).first()
    // println
    println(s"Content Size Avg : ${contentSizeRow.getLong(0)/contentSizeRow.getLong(1)}, Min: ${contentSizeRow(2)}, Max: ${contentSizeRow(3)}")


    /**
      * 需求二：Response Code
      *     A count of response code's returned.
      *
      */
    val responseCodeToCount: Array[(Int, Long)] = sqlContext.sql(
      """
        |SELECT
        | responseCode, COUNT(*)
        |FROM
        | tmp_access_log
        |GROUP By
        | responseCode
      """.stripMargin).map(row => (row.getInt(0) , row.getLong(1))).collect()
    println(s"Response Code Count : ${responseCodeToCount.mkString("[", ",", "]")}")

    /**
      * 需求三：IP Address
      *     All IP Addresses that have accessed this server more than N times.
      */
    val ipAddresses: Array[String] = sqlContext.sql(
      """
        |SELECT
        | ipAddress, COUNT(*) AS cnt
        |FROM
        | tmp_access_log
        |GROUP By
        | ipAddress
        |HAVING
        | cnt > 30
        |LIMIT
        | 10
      """.stripMargin).map(row => row.getString(0)).collect()
    println(s"IP Addresses: ${ipAddresses.mkString("[", ",", "]")}")


    /**
      * 需求四：Endpoint
      *     The top endpoints requested by count
      */
    val topEndpoints = sqlContext.sql(
      """
        |SELECT
        | endpoint, COUNT(*) AS cnt
        |FROM
        | tmp_access_log
        |GROUP By
        | endpoint
        |ORDER By
        | cnt DESC
        |LIMIT 5
      """.stripMargin).map(row => (row.getString(0) , row.getLong(1))).collect()
    println(s"Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}")

    // 当表使用结束以后，释放内存
    sqlContext.uncacheTable("tmp_access_log")

/** ===============================================================================*/
    // WEB UI
    Thread.sleep(100000)

    // 关闭资源
    sc.stop()
  }

}
