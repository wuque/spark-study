package com.hpsk.bigdata.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于JAVA实现SparkSQL数据分析，主要是如何通过JAVA API创建DataFrame，尤其RDD转换为DataFrame
 */
public class JavaSQLPageViewAnalyzer {
    /**
     * Driver Program:
     *      JavaSparkContext
     * @param args
     */
    public static void main(String[] args) {
        // 创建SparkConf, 配置信息
        SparkConf sparkConf = new SparkConf()
                // 设置应用的名称
                .setAppName(JavaSQLPageViewAnalyzer.class.getSimpleName())
                // 设置程序的运行模式，注意在实际环境部署的时候，通过命令行设置的
                .setMaster("local[2]") ;
        // 创建JavaSparkContext, 构建SparkApplication入口
        JavaSparkContext sc = new JavaSparkContext(sparkConf) ;

        // 设置日志级别
        sc.setLogLevel("WARN");

        /**
         * 创建SparkSQL程序入口SQLContext，读取数据，创建DataFrame
         */
        SQLContext sqlContext = new SQLContext(sc) ;

/** ==========================================================================
  *                         自定义函数（UDF和UDAF)
  * ========================================================================== */
        sqlContext.udf().register(
                "toUpper", // function name
                new UDF1<String, String>() {
                    // call method
                    public String call(String word) throws Exception {
                        return word.toUpperCase();
                    }
                }, // function
                DataTypes.StringType // return Type
        );


/** ========================================================================== */
        /**
         * 分析淘宝用户行为数据
         *      HDFS：/user/hive/warehouse/page_views
         *  字段信息：每行数据的各个字段之间使用制表符进行分割的
                 track_time              string
                 url                     string
                 session_id              string
                 referer                 string
                 ip                      string
                 end_user_id             string
                 city_id                 string
         *
         */
        JavaRDD<String> pageViewRDD = sc.textFile("/user/hive/warehouse/page_views") ;

        // 将RDD -> DataFrame
        /**
         * 采用自定义schema方式将RDD转换为DataFrame
         *      -1, RDD[Row]
         *      -2, schema
         *      -3, sqlContext.createDataFrame(rowRDD, schema)
         */
        JavaRDD<Row> rowRDD = pageViewRDD.map(
            new Function<String, Row>() {
                // call method
                public Row call(String line) throws Exception {
                    // 按照 制表符进行分割
                    String[] array = line.split("\t") ;
                    // Row: RowFactory.create
                    return RowFactory.create(
                        array[0], array[1],array[2],array[3],array[4],array[5],array[6]
                    );
                }
            }
        );

        // definition schema
        List<StructField> fields = new ArrayList<StructField>() ;
        // add StructField
        fields.add(DataTypes.createStructField("track_time", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("session_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("referer", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("end_user_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("city_id", DataTypes.StringType, true));

        /// 创建StructType
        StructType schema = DataTypes.createStructType(fields) ;

        // 创建DataFrame
        DataFrame pageViewDF = sqlContext.createDataFrame(rowRDD, schema) ;

        // print schema
        pageViewDF.printSchema();
        pageViewDF.show();

        System.out.println("==================================================");
        /**
         * 基于SQL进行数据分析
         */
        // 注册DataFrame为临时表，以便使用SQL进行分析
        pageViewDF.registerTempTable("tmp_page_view");
        // 编写SQL语句
        // 需求：按照session_id进行分组统计，然后对统计的值进行降序排序，获取前10个值
        // 会话PV
        String sqlStr =
                "SELECT " +
                        "session_id, COUNT(*) AS cnt " +
                "FROM " +
                        "tmp_page_view " +
                "GROUP BY " +
                        "session_id " +
                "ORDER BY " +
                        "cnt DESC " +
                "LIMIT 10" ;
        // 执行SQL语句
        DataFrame sessionIdCountDF = sqlContext.sql(sqlStr) ;
        // 显示
        sessionIdCountDF.show();
        System.out.println("==================================================");
        // 将DataFrame转换为RDD
        sessionIdCountDF
                .javaRDD()
                .map(
                        new Function<Row, String>() {
                            // call method
                            public String call(Row row) throws Exception {
                                return row.getString(0) + " -> " + row.getLong(1);
                            }
                        }
                )
                .foreach(
                        new VoidFunction<String>() {
                            public void call(String str) throws Exception {
                                System.out.println(str);
                            }
                        }
                );

        System.out.println("==================================================");
        /**
         * 基于DSL进行数据分析
         */
        DataFrame sessionPvDF = pageViewDF
                .groupBy("session_id")
                .count() // 调用此函数以后，统计的列名为"count"
                .sort(new ColumnName("count").desc())
                .limit(10) ;
        // iterator lsit
        for(Row row: sessionPvDF.collect()){
            System.out.println(row.getString(0) + " -> " + row.getLong(1));
        }

        /**
         * 使用自定义函数
         */
        pageViewDF
                .selectExpr("session_id", "toUpper(session_id) AS uuid")
                .show(10, false);

        /**
         * 读取JSON格式的数据
         */
        DataFrame jsonDF = sqlContext.read().json("/datas/resources/people.json");
        jsonDF.show();

        /**
         * 读取parquet格式的数
         */
        DataFrame parquetDF = sqlContext.read().parquet("/datas/resources/users.parquet") ;
        parquetDF.show();

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
