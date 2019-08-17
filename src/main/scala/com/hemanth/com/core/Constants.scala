package com.hemanth.com.core

class Constants (config : Config){

  val CONNECTION_STRING = config.get("connection.string",null)
  val TABLE_NAME_WITH_SCHEMA = config.get("jdbc.table.name",null)
  val USER_ID          = config.get("jdbc.user.id",null)
  val PASSWORD          = config.get("jdbc.password",null)
  val HDFS_PATH         = config.get("hdfs.location",null)
  val FORMAT            = config.get("file.format",null)
  val MODE              = config.get("write.mode",null)
}