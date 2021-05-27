package com.ldg.gues

import java.sql.{Connection, DriverManager, PreparedStatement}

object MysqlUtil {

  //  windows下,
  val url = "jdbc:mysql://192.168.249.1:3306/dx?useUnicode=true&characterEncoding=UTF-8"
  val prop = new java.util.Properties
  prop.setProperty("user", "root")
  prop.setProperty("password", "root")

  /**
    * cleanMap表插入数据
    *
    * @param iterator
    */

  def cleanMap(iterator: Iterator[(String, String, String, String, String, String, String, String)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "replace into cleanMap values (?, ?, ?, ?, ?, ?, ?, ?)"
    conn = DriverManager.getConnection(url, prop)
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(sql)
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.setString(3, data._3)
      ps.setString(4, data._4)
      ps.setString(5, data._5)
      ps.setString(6, data._6)
      ps.setString(7, data._7)
      ps.setString(8, data._8)
      ps.addBatch();
    })
    ps.executeBatch()
    conn.commit()
    if (ps != null) {
      ps.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  /**
    * 插入数据 sum表
    *
    * @param iterator
    */
  def addsum(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "replace into sum values (?, ?)"
    conn = DriverManager.getConnection(url, prop)
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(sql)
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setInt(2, data._2)
      ps.addBatch();
    })
    ps.executeBatch()
    conn.commit()
    if (ps != null) {
      ps.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  /**
    *
    *  批量添加插入按区域和渠道汇总数据
    * @param iterator
    */

  def add_table_areacodeAndchannelnoMap(iterator: Iterator[(String, String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "replace into a_c_map values (?, ?, ?)"
    conn = DriverManager.getConnection(url, prop)
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(sql)
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.setInt(3, data._3)
      ps.addBatch();
    })
    ps.executeBatch()
    conn.commit()
    if (ps != null) {
      ps.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  /**
    *   批量添加按区域和请求类型汇总数据
    * @param iterator
    */
  def add_table_areacodeAndrequesttypeMap(iterator: Iterator[(String, String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "replace into a_r_map values (?, ?, ?)"
    conn = DriverManager.getConnection(url, prop)
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(sql)
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.setInt(3, data._3)
      ps.addBatch();
    })
    ps.executeBatch()
    conn.commit()
    if (ps != null) {
      ps.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

  /**
    *
    * 批量添加插入按 用户 渠道汇总数据
    * @param iterator
    */
  def add_table_userInfoMap(iterator: Iterator[(String, Int, String, Long)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "replace into userInfoMap values (?, ?, ?, ?)"
    conn = DriverManager.getConnection(url, prop)
    conn.setAutoCommit(false)
    ps = conn.prepareStatement(sql)
    iterator.foreach(data => {
      ps.setString(1, data._1)
      ps.setInt(2, data._2)
      ps.setString(3, data._3)
      ps.setLong(4, data._4)
      ps.addBatch();
    })
    ps.executeBatch()
    conn.commit()
    if (ps != null) {
      ps.close()
    }
    if (conn != null) {
      conn.close()
    }
  }

}