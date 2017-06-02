package com.hpsk.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random


/**
  * 类似于MapReduce中的二次排序，分组、排序、TopKey
  */
object GroupSortTopSpark {

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
        .setAppName("GroupSortTopSpark")
        // 设置程序运行的环境，通常情况下，在IDE中开发的时候设置为local mode，
        // 在实际部署的时候将通过提交应用的命令行进行设置
        .setMaster("local[2]")
    // 创建SparkContext 上下文对象
    val sc = SparkContext.getOrCreate(sparkConf)

    // 关闭日志
    sc.setLogLevel("WARN")

/** =============================================================================== */
    /**
      * Step 1: read data
      *     SparkContext用户读取数据
      */
    val rdd = sc.textFile("D:/group.txt")

    // println(rdd.count() + "\n" + rdd.first())

    /**
      * Step 2: process data
      *   RDD#transformation
      *
      *   先按第一个字段进行分组，接着对第二个字段进行排序获取前3个最大的值
      */
    val sortTopRdd: RDD[(String, List[Int])] = rdd
      .map(line => {
        // 进行分割
        val splited = line.split(" ")
        // return
        (splited(0).toString, splited(1).toInt)
      })
      // 分组
      .groupByKey()  // RDD[(String, Iterable[Int])]
      // 对组内数据进行降序排序
      .map{
        case (key, iter) => {
          // 组内排序
          val sortList: List[Int] = iter.toList.sorted.takeRight(3).reverse
          // 返回
          (key, sortList)
        }
      }

    // 打印信息
    sortTopRdd.foreach(println)


    println("进行二次聚合操作的..................................")

    /**
      * 考虑到 分组可能会引起 数据倾斜，采用两阶段聚合进行操作。
      */
    rdd
      .map(line => {
        // 进行分割
        val splited = line.split(" ")
        // return
        (splited(0).toString, splited(1).toInt)
      })
      /**
        * 进行第一次分组聚合
        */
      // 给 原始Key 加上 前缀
      .map(tuple => {
        // 随机数实例
        val random = new Random(2)
        //
        (random + "_" + tuple._1, tuple._2)
      })
      // 第一次分组
      .groupByKey()  // RDD[(String, Iterable[Int])]
      // 对组内数据进行降序排序
      .map{
        case (key, iter) => {
          // 组内排序
          val sortList: List[Int] = iter.toList.sorted.takeRight(3).reverse
          // 返回
          (key, sortList)
        }
      }
      /**
        * 进行第二次分组聚合
        */
      .map{
        case (key, iter) => {
          (key.split("_")(1), iter)
        }
      }
      // 第二次分组
      .groupByKey()  // RDD[(String, Iterable[Iterable[Int]])]
      // 对组内数据进行降序排序
      .map{
        case (key, iter) => {
          val sortList: List[Int] = iter.toList.flatMap(_.toList).sorted.takeRight(3).reverse
          // 返回
          (key, sortList)
        }
    }.foreach(println)


    println("进行二次聚合操作的.......................调整代码..")
    rdd
      /**
        * 进行第一次分组聚合
        */
      .map(line => {
        // 随机数实例
        val random = new Random(2)
        // 进行分割
        val splited = line.split(" ")
        // 给 原始Key 加上 前缀
        (random + "_" + splited(0), splited(1).toInt)
      })
      // 第一次分组
      .groupByKey()  // RDD[(String, Iterable[Int])]
      // 对组内数据进行降序排序
      .map{
        case (key, iter) => {
          // 组内排序
          val sortList: List[Int] = iter.toList.sorted.takeRight(3).reverse
          // 返回
          (key.split("_")(1), sortList)
        }
      }
      /**
        * 进行第二次分组聚合
        */
      // 第二次分组
      .groupByKey()  // RDD[(String, Iterable[Iterable[Int]])]
      // 对组内数据进行降序排序
      .map{
        case (key, iter) => {
          val sortList: List[Int] = iter.toList.flatMap(_.toList).sorted.takeRight(3).reverse
          // 返回
          (key, sortList)
        }
      }.foreach(println)
/** =============================================================================== */

    Thread.sleep(1000000)

    // 关闭资源
    sc.stop()
  }

}
