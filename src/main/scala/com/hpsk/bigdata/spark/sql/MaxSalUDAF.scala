package com.hpsk.bigdata.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}


/**
  * 自定义用户UDAF，实现获取最大值
  */
object MaxSalUDAF extends UserDefinedAggregateFunction{

  /**
    * 指定输入字段的类型和名称
    * @return
    */
  override def inputSchema: StructType = StructType(
    Array(StructField("sal", DoubleType, true))    // 输入字段，依据函数的使用场景而定
  )

  /**
    * 依据实际需求，定义缓冲字段的名称和类型
    * @return
    */
  override def bufferSchema: StructType = StructType(
    StructField("max_sal", DoubleType, true) :: Nil
  )

  /**
    * 对缓冲数据的字段的值进行初始化
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, 0.0)

  /**
    * 当数据进行函数以后，需要更新缓冲数据里的值
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 获取缓冲的值
    var bufferSal = buffer.getDouble(0)

    // 获取输入的值
    val inputSal = input.getDouble(0)

    // 更新缓冲的值
    if (bufferSal < inputSal){
      bufferSal = inputSal
    }

    buffer.update(0, bufferSal)
  }


  /**
    * 合并多个分区中缓冲的值
    *
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 分别获取两个缓冲数据中的值
    var buffer1Sal = buffer1.getDouble(0)
    val buffer2Sal = buffer2.getDouble(0)

    // 比较大小
    if(buffer1Sal < buffer2Sal){
      buffer1Sal = buffer2Sal
    }

    // 更新缓冲值
    buffer1.update(0, buffer1Sal)
  }

  /**
    * 确定唯一性
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 指定数据的类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    * 决定最后如何进行输出
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}
