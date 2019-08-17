package com.hemanth.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Db2tohdfsapp extends App {
  val conf = new SparkConf().setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val db2ToHdfs = new Db2tohdfs(spark)
  db2ToHdfs.process(args)
  spark.stop()
}