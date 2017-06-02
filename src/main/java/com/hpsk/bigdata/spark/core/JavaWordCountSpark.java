package com.hpsk.bigdata.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * 使用Java语言进行SparkCore程序开发
 *      实现大数据框架的经典案例：词频统计WordCount
 */
public class JavaWordCountSpark {
    /**
     * Driver Program:
     *      JavaSparkContext
     * @param args
     */
    public static void main(String[] args) {
        // 创建SparkConf, 配置信息
        SparkConf sparkConf = new SparkConf()
                // 设置应用的名称
                .setAppName(JavaWordCountSpark.class.getSimpleName())
                // 设置程序的运行模式，注意在实际环境部署的时候，通过命令行设置的
                .setMaster("local[2]") ;
        // 创建JavaSparkContext, 构建SparkApplication入口
        JavaSparkContext sc = new JavaSparkContext(sparkConf) ;

        // 设置日志级别
        // Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
        sc.setLogLevel("WARN");
/** ========================================================================== */
        // Step 1: input data -> RDD
        JavaRDD<String> inputRDD = sc.textFile("/datas/wc.input") ;

        // 测试
        System.out.println("Count: " + inputRDD.count());
        System.out.println("First: \n" + inputRDD.first());

        // Step 2: process data -> RDD#API
        /**
         * TODO: scala
         *      rdd.flatMap(line => line.split(" "))
         */
        JavaRDD<String> wordRDD = inputRDD.flatMap(
                new FlatMapFunction<String, String>(){
                    // call method
                    public Iterable<String> call(String line) throws Exception {
                        // List本身就Iterable，将数组转换为List
                        return Arrays.asList(line.split(" "));
                    }
                }
        );

        /**
         * TODO: scala
         *      rdd.map(word => (word, 1))
         */
        JavaPairRDD<String, Integer> tupleRDD = wordRDD.mapToPair(
                new PairFunction<String, String, Integer>(){
                    // call method
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                }
        ) ;

        /**
         * TODO: scala
         *      rdd.reduceByKey((a, b) => a + b)
         */
        JavaPairRDD<String, Integer> wordCountRDD = tupleRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>(){
                    // call method
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );


        // Step 3: output data
        /**
         * TODO: scala
         *       rdd.collect
         */
        List<Tuple2<String, Integer>> outputList = wordCountRDD.collect() ;
        // iterator
        for(Tuple2<String, Integer> output: outputList){
            System.out.println(output._1() + " : " + output._2());
        }

        // 将结果保存到HDFS文件中
        wordCountRDD
            .map(
                new Function<Tuple2<String, Integer>, String>(){
                    // call method
                    public String call(Tuple2<String, Integer> tuple) throws Exception {
                        return tuple._1() + "\t" + tuple._2();
                    }
                }
            )
            .saveAsTextFile("/datas/wc-java-output-" + System.currentTimeMillis());

        /**
         * 针对Count进行降序排序，使用top方法
         */
        /**
         * TODO: scala
         *      rdd.top(3)(OrderingUtils.SecondValueOrdering)
         */
        List<Tuple2<String, Integer>> topList = wordCountRDD.top(
                3, // the top k
                new SecondValueComparator() // 自定义比较器
                /**
                    new Comparator<Tuple2<String, Integer>>(){
                        // compare method
                        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                            return o1._2().compareTo(o2._2());
                        }
                    }
                */
        );
        // iterator
        for(Tuple2<String, Integer> output: topList){
            System.out.println(output._1() + " : " + output._2());
        }

        /**
         * TODO: 作业：将wordCountRDD结果存储到MySQL数据库表中，采用JDBC方式
         */
        wordCountRDD.foreachPartition(  // 针对RDD中每个分区的数据进行操作
                new VoidFunction<Iterator<Tuple2<String, Integer>>>(){
                    // call method
                    public void call(Iterator<Tuple2<String, Integer>> iter) throws Exception {
                        // TODO: 1-> JDBC Database Connection

                        // iterator
                        while(iter.hasNext()){
                            Tuple2<String, Integer> tuple = iter.next();
                            // TODO: 2 -> Insert Table
                        }

                        // TODO: 3 -> Close Database Connection
                    }
                }
        );

/** ========================================================================== */
        // Monitor Spark Application WEB UI 4040
        try{
            Thread.sleep(1000000);
        }catch (Exception e){
            System.out.println(e);
        }

        // SparkContext Stop
        sc.stop();
    }


    /**
     * 自定义比较器，比较两个Tuple2中的第二个元素的大小，记住一定要实现 Serializable
     */
    static class SecondValueComparator
            implements Comparator<Tuple2<String, Integer>>, Serializable{
        // compare method
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            return o1._2().compareTo(o2._2());
        }
    }

}
