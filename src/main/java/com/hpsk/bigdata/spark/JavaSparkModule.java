package com.hpsk.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 使用Java语言进行SparkCore程序开发
 *      建议：
 *          - 在编程的时候，对比SCALA语言开发
 *          - 一定要看源码，非常重要
 *      关键点：
 *          SCALA 高阶函数  -> 匿名内部类(call方法)
 *      备注：
 *          SCALA 基于JVM之上的语言，与JAVA语言相互调用
 */
public class JavaSparkModule {
    /**
     * Driver Program:
     *      JavaSparkContext
     * @param args
     */
    public static void main(String[] args) {
        // 创建SparkConf, 配置信息
        SparkConf sparkConf = new SparkConf()
                // 设置应用的名称
                .setAppName(JavaSparkModule.class.getSimpleName())
                // 设置程序的运行模式，注意在实际环境部署的时候，通过命令行设置的
                .setMaster("local[2]") ;
        // 创建JavaSparkContext, 构建SparkApplication入口
        JavaSparkContext sc = new JavaSparkContext(sparkConf) ;

/** ========================================================================== */
        // TODO: do something


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

}
