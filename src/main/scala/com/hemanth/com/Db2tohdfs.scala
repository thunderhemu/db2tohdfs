package com.hemanth.com

import com.hemanth.com.core.{Config, Constants}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

class Db2tohdfs(spark : SparkSession) extends LogSupport{

  def process(args : Array[String]) : Unit = {

    logger.info(" Appliction Started ")
    if (args.length !=1)
      throw new IllegalArgumentException("Invalid number of arguments, the script accepts config placed in hdfs ")
    logger.info(" Application valid arguments it is "+args(0))
    val constants = validation(args(0))
    writeDf(rdbmsRead(constants),constants)
  }


  def fileExists(path: String ): Boolean = {
    val hconf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hconf)
    fs.exists(new Path(path))
  }

  def validation(configPath : String) : Constants = {
    if (!fileExists(configPath))
      throw new IllegalArgumentException("Invalid config file path, kindly provide valid path!! ")
    val config = new Config
    config.loadConfig(configPath,spark.sparkContext)
    val constants = new Constants(config)
    if ( constants.CONNECTION_STRING == null)
      throw new IllegalArgumentException(" Invalid connection string, connection can't be null!! ")
    if ( constants.TABLE_NAME_WITH_SCHEMA == null )
      throw new IllegalArgumentException(" Invalid rdbms table name, rdbms table name can't be null!! ")
    if ( constants.MODE == null )
      throw new IllegalArgumentException (" Invalid mode, mode can't be null!! ")
    if (constants.FORMAT == null)
      throw new IllegalArgumentException (" Invalid file format, file format can't be null!! ")
    if (constants.MODE.toLowerCase != "append" && constants.MODE.toLowerCase != "overwrite")
      throw new IllegalArgumentException (" Invalid mode, mode must be either append or overwrite!! ")
    if (constants.USER_ID == null)
      throw new IllegalArgumentException("Invalid user id, user id can't be null")
    constants
  }

  // reas from rdbms
  def rdbmsRead(constants: Constants): DataFrame = {
    spark.read.format("jdbc").option("url", constants.CONNECTION_STRING).
      option("driver", "com.ibm.db2.jcc.DB2Driver").option("dbtable", constants.TABLE_NAME_WITH_SCHEMA).option("user", constants.USER_ID).
      option("password", constants.PASSWORD).load()
  }

  // write to hdfs location
  def writeDf(df: DataFrame, constants: Constants): Unit ={
    logger.info(constants.MODE+"ing the data in the format "+constants.FORMAT + " to the location "+constants.HDFS_PATH)
    constants.FORMAT.toLowerCase match {
      case "paquet" => df.write.mode(constants.MODE.toLowerCase).parquet(constants.HDFS_PATH)
      case "csv"    => df.write.mode(constants.MODE.toLowerCase).csv(constants.HDFS_PATH)
      case "orc"    => df.write.mode(constants.MODE.toLowerCase).orc(constants.HDFS_PATH)
      case "json"   => df.write.mode(constants.MODE.toLowerCase).json(constants.HDFS_PATH)
      case _       => throw new IllegalArgumentException("Invalid file format, currently we support parquet,orc,csv,json only!! ")
    }
  }

}
