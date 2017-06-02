package com.hpsk.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

/**
 * 使用Java语言进行编程实现SparkStreaming的WordCount程序，从NetWork读取数据
 *
 *    作业：
 *          为了让大家更加的熟练使用JAVa语言开发SparkStreaming编程：
 *          使用JAVa实现从Kafka Topic中读取数据，并且进行实时状态更新统计
 *              -1, kafka direct
 *              -2, updateStateByKey
 *              -3, optional：SparkStreak HA
 *              -4, window
 */
public class JavaNetWorkWordCount {
    /**
     * Driver Program:
     *      JavaSparkContext
     * @param args
     */
    public static void main(String[] args) {
        // 创建SparkConf, 配置信息
        SparkConf sparkConf = new SparkConf()
                // 设置应用的名称
                .setAppName(JavaNetWorkWordCount.class.getSimpleName())
                // 设置程序的运行模式，注意在实际环境部署的时候，通过命令行设置的
                .setMaster("local[2]") ;
        // 创建JavaSparkContext, 构建SparkApplication入口
        JavaSparkContext sc = new JavaSparkContext(sparkConf) ;

        sc.setLogLevel("WARN");

/** ========================================================================== */
        /**
         * JavaStreamingContext，SparkStreaming程序的入口
         *      实时接收到流式的数据
         *      指定每批次的时间间隔 batch interval
         */
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));

        /**
         * 从Socket读取数据
         *      Create an input stream from network source hostname:port.
         */
        JavaReceiverInputDStream<String> inputDStream = jssc.socketTextStream(
                "bigdata-training01.hpsk.com", 9999
        ) ;


        /**
         * Split each line into words
         */
        JavaDStream<String> wordsDStream = inputDStream.flatMap(
                new FlatMapFunction<String, String>() {
                    // call method
                    public Iterable<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" "));
                    }
                }
        );

        /**
         * Count batch word in each batch
         */
        JavaPairDStream<String, Integer> pairDStream = wordsDStream.mapToPair(
                new PairFunction<String, String, Integer>() {
                    // call method
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }
                }
        );
        JavaPairDStream<String, Integer> wordCountDStream =pairDStream.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    // call method
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        /**
         * 控制台进行打印接收的数据
         */
        // wordCountDStream.print();

        /**
         * DStream#foreachRDD
         */
        wordCountDStream.foreachRDD(
            new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
                // call method
                public void call(JavaPairRDD<String, Integer> rdd, Time batchTime) throws Exception {
                    // 对批次时间进行转换
                    final String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            .format(new Date(batchTime.milliseconds())) ;
                    // 由于每批次的数据量不多，并且进行处理分析以后 结果及数据也不多
                    rdd.coalesce(1).foreachPartition(
                        new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                            // call method
                            public void call(Iterator<Tuple2<String, Integer>> iter) throws Exception {
                                // TODO: 将结果集存储到Redis数据库中，此处为方便，打印即可
                                System.out.println("Batch Time: " + time);
                                // 迭代操作
                                while(iter.hasNext()){
                                    // 获取分区中的数据
                                    Tuple2<String, Integer> tuple = iter.next();
                                    // 打印
                                    System.out.println("\t" + tuple._1() + " : " + tuple._2());
                                }
                            }
                        }
                    );
                }
            }
        );




        /**
         * Start Streaming Application
         */
        jssc.start();
        jssc.awaitTermination();

/** ========================================================================== */
        // Monitor Spark Application WEB UI 4040
        try{
            Thread.sleep(1000000);
        }catch (Exception e){
            System.out.println(e);
        }

        // SparkContext Stop
        jssc.stop();
    }

}
