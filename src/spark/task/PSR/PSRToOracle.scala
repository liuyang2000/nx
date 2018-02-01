package spark.task.PSR

import org.apache.spark.sql.DataFrame
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Connection
import spark.util.utils
import spark.util.OracleUtils

object PSRToOracle {
  def tmnl(data: DataFrame) {
    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = """insert into pwyw.A_OUT_TMNL_ONOFF_STAT_D (TERMINAL_ADDR,ORG_NO,TJRQ,EVENT_OFF_CNT,EVENT_ON_CNT,FIX_OFF_CNT,FIX_ON_CNT,COMM_ONLINE_FLAG,DAY_NOPOWER_FLAG,
        LOSS_CURVE_POINTS,POWEROFF_TOTAL_TIME,CIS_ORG_NO,PI_ORG_NO,FACTORY_CODE,CIS_ASSET_NO,ASSET_NO,LOT_ID) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def tgdetail(data: DataFrame, statDate: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.A_FIX_TG_DETAIL_STAT_D where TJRQ = """ + s"to_date('$statDate','yyyyMMdd')"
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = """insert into pwyw.A_FIX_TG_DETAIL_STAT_D (TG_ID,LINE_SEG_NO,PI_LINE_NO,PI_ORG_NO,TG_CAP,POWEROFF_TYPE,PLAN_OFF_TYPE,POWEROFF_TOTAL_TIME,LOSS_POWER,POWEROFF_TIME,
        POWERON_TIME,LINESEG_OFF_ID,DATA_ID,TJRQ) values(?,?,?,?,?,?,?,?,?,?,?,?,?,""" + s"to_date('$statDate','yyyyMMdd'))"
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def linesegdetail(data: DataFrame, statDate: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.A_FIX_LINESEG_DETAIL_STAT_D where TJRQ = """ + s"to_date('$statDate','yyyyMMdd')"
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = """insert into pwyw.A_FIX_LINESEG_DETAIL_STAT_D (LINE_SEG_NO,PI_ORG_NO,LINESEG_OFF_ID,PI_LINE_NO,POWEROFF_TIME,POWERON_TIME,POWEROFF_TYPE,PLAN_OFF_TYPE,
        POWEROFF_TOTAL_TIME,LOSS_POWER,LINE_OFF_ID,DATA_ID,POWEROFF_ALONE,POWEROFF_CIS_CONS_CNT,POWEROFF_PI_CONS_CNT,PI_CONS_CNT,INTEGRITY_RATE,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" + s"to_date('$statDate','yyyyMMdd'))"
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def linedetail(data: DataFrame, statDate: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.A_FIX_LINE_DETAIL_STAT_D where TJRQ = """ + s"to_date('$statDate','yyyyMMdd')"
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = """insert into pwyw.A_FIX_LINE_DETAIL_STAT_D (PI_LINE_NO,PI_ORG_NO,LINE_OFF_ID,POWEROFF_TIME,POWERON_TIME,POWEROFF_TYPE,PLAN_OFF_TYPE,POWEROFF_TOTAL_TIME,
        LOSS_POWER,DATA_ID,POWEROFF_CIS_CONS_CNT,POWEROFF_PI_CONS_CNT,PI_CONS_CNT,INTEGRITY_RATE,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" + s"to_date('$statDate','yyyyMMdd'))"
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def tgtotal(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.drop("TG_CAP").drop("PI_LINE_NO").drop("POWEROFF_CNT")
      .foreachPartition(row => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = s"""insert into pwyw.$tableName (PBID,SSXL,DWBM,JHTDCS,JHTDSC,JHTDSSDL,LSJXTDCS,LSJXTDSC,LSJXTDSSDL,GZTDCS,GZTDSC,GZTDSSDL,PBMC,ZXMC,
          SSGT,GTMC,DWMC,BZID,TQDZ,TQBS,SCCJ,CCBH,CCRQ,YXZT,DQTZ,TYRQ,ZBID,DWJB,SJDWBM,SJDWMC,TQDYYHS,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" + (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
        try {
          Class.forName(utils.cp.getProperty("driver"))
          conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
          ps = conn.prepareStatement(sql)
          conn.setAutoCommit(false)
          var iCnt: Int = 0
          row.foreach(data => {
            iCnt += 1
            OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 18, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 19, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 20, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 21, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 22, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 23, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 24, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 25, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 26, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 27, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 28, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 29, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 30, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 31, ps, java.sql.Types.NUMERIC)
            ps.addBatch()

            if (iCnt % 10000 == 0 && iCnt != 0) {
              println(s"============================$iCnt")
              ps.executeBatch()
              ps.clearBatch()
              conn.commit()
            }
          })

          println(s"============================$iCnt")
          ps.executeBatch()
          ps.clearBatch()
          conn.commit()

        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })
  }
  
  def consTotal(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close
  
    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (
        CONS_NO,DWBM,TDCS,JHTDCS,JHTDSC,
        JHTDSSDL,LSJXTDCS,LSJXTDSC,LSJXTDSSDL,GZTDCS,
        GZTDSC,GZTDSSDL,CONS_NAME,ELEC_ADDR,YXTQID,
        YXTQMC,PMS_BYQ_ID,PMS_BYQ_MC,DQTZ,BZID,
        CNW,DWMC,DWJB,SJDWBM,SJDWMC,XR_SJ,TJRQ) 
      values(?,?,?,?,?,
             ?,?,?,?,?,
             ?,?,?,?,?,
             ?,?,?,?,?,
             ?,?,?,?,?,sysdate,""" + (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 18, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 19, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 20, ps, java.sql.Types.VARCHAR)
          
          OracleUtils.ExecPS(data, 21, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 22, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 23, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 24, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 25, ps, java.sql.Types.VARCHAR)
          
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }
  

  def tgtotalW(data: DataFrame, tableName: String, statDateS: String, statDateE: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDateS','yyyyMMdd')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.drop("TG_CAP").drop("PI_LINE_NO").drop("POWEROFF_CNT")
      .foreachPartition(row => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = s"""insert into pwyw.$tableName (PBID,SSXL,DWBM,JHTDCS,JHTDSC,JHTDSSDL,LSJXTDCS,LSJXTDSC,LSJXTDSSDL,GZTDCS,GZTDSC,GZTDSSDL,PBMC,ZXMC,
          SSGT,GTMC,DWMC,BZID,TQDZ,TQBS,SCCJ,CCBH,CCRQ,YXZT,DQTZ,TYRQ,ZBID,DWJB,SJDWBM,SJDWMC,TQDYYHS,TJKSRQ,TJJSRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDateS','yyyyMMdd'),to_date('$statDateE','yyyyMMdd'))"""
        try {
          Class.forName(utils.cp.getProperty("driver"))
          conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
          ps = conn.prepareStatement(sql)
          conn.setAutoCommit(false)
          var iCnt: Int = 0
          row.foreach(data => {
            iCnt += 1
            OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 18, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 19, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 20, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 21, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 22, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 23, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 24, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 25, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 26, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 27, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 28, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 29, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 30, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 31, ps, java.sql.Types.NUMERIC)
            ps.addBatch()

            if (iCnt % 10000 == 0 && iCnt != 0) {
              println(s"============================$iCnt")
              ps.executeBatch()
              ps.clearBatch()
              conn.commit()
            }
          })

          println(s"============================$iCnt")
          ps.executeBatch()
          ps.clearBatch()
          conn.commit()

        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })
  }
  
  def constotalW(data: DataFrame, tableName: String, statDateS: String, statDateE: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDateS','yyyyMMdd')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = s"""insert into pwyw.$tableName (
            CONS_NO,DWBM,TDCS,JHTDCS,JHTDSC,
            JHTDSSDL,LSJXTDCS,LSJXTDSC,LSJXTDSSDL,GZTDCS,
            GZTDSC,GZTDSSDL,CONS_NAME,ELEC_ADDR,YXTQID,
            YXTQMC,PMS_BYQ_ID,PMS_BYQ_MC,DQTZ,BZID,
            CNW,DWMC,DWJB,SJDWBM,SJDWMC,XR_SJ,
            TJKSRQ,TJJSRQ) 
        values(?,?,?,?,?,
               ?,?,?,?,?,
               ?,?,?,?,?,
               ?,?,?,?,?,
               ?,?,?,?,?,sysdate,
               to_date('$statDateS','yyyyMMdd'),to_date('$statDateE','yyyyMMdd'))"""
        try {
          Class.forName(utils.cp.getProperty("driver"))
          conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
          ps = conn.prepareStatement(sql)
          conn.setAutoCommit(false)
          var iCnt: Int = 0
          row.foreach(data => {
            iCnt += 1
            
            OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
            
            OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
            
            OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
            
            OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 18, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 19, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 20, ps, java.sql.Types.VARCHAR)
            
            OracleUtils.ExecPS(data, 21, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 22, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 23, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 24, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 25, ps, java.sql.Types.VARCHAR)
            
            ps.addBatch()

            if (iCnt % 10000 == 0 && iCnt != 0) {
              println(s"============================$iCnt")
              ps.executeBatch()
              ps.clearBatch()
              conn.commit()
            }
          })

          println(s"============================$iCnt")
          ps.executeBatch()
          ps.clearBatch()
          conn.commit()

        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })
  }

  def linesegtotal(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.drop("PI_LINE_NO").drop("POWEROFF_CNT").drop("POWEROFF_ALONE_CONS_CNT").drop("POWEROFF_NORMAL_CNT")
      .foreachPartition(row => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = s"""insert into pwyw.$tableName (XLID,DWBM,JHTDCS,JHTDSC,JHTDSSDL,GZTDCS,GZTDSC,GZTDSSDL,LSJXTDCS,LSJXTDSC,LSJXTDSSDL,XLMC,YXBH,ZXMC,
          SSZX,BZID,SSDZ,XLZCD,JKXLCD,DLXLCD,JKJXFS,DLJXFS,ZDYXDL,JJDL,YXFHXE,DQTZ,DWJB,SJDWBM,SJDWMC,GBHS,ZBHS,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" + (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
        try {
          Class.forName(utils.cp.getProperty("driver"))
          conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
          ps = conn.prepareStatement(sql)
          conn.setAutoCommit(false)
          var iCnt: Int = 0
          row.foreach(data => {
            iCnt += 1
            OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 18, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 19, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 20, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 21, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 22, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 23, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 24, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 25, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 26, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 27, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 28, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 29, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 30, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 31, ps, java.sql.Types.NUMERIC)
            ps.addBatch()

            if (iCnt % 10000 == 0 && iCnt != 0) {
              println(s"============================$iCnt")
              ps.executeBatch()
              ps.clearBatch()
              conn.commit()
            }
          })

          println(s"============================$iCnt")
          ps.executeBatch()
          ps.clearBatch()
          conn.commit()

        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })
  }

  def linesegtotalW(data: DataFrame, tableName: String, statDateS: String, statDateE: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDateS','yyyyMMdd')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.drop("PI_LINE_NO").drop("POWEROFF_CNT").drop("POWEROFF_ALONE_CONS_CNT").drop("POWEROFF_NORMAL_CNT")
      .foreachPartition(row => {
        var conn: Connection = null
        var ps: PreparedStatement = null
        val sql = s"""insert into pwyw.$tableName (XLID,DWBM,JHTDCS,JHTDSC,JHTDSSDL,GZTDCS,GZTDSC,GZTDSSDL,LSJXTDCS,LSJXTDSC,LSJXTDSSDL,XLMC,YXBH,ZXMC,
          SSZX,BZID,SSDZ,XLZCD,JKXLCD,DLXLCD,JKJXFS,DLJXFS,ZDYXDL,JJDL,YXFHXE,DQTZ,DWJB,SJDWBM,SJDWMC,GBHS,ZBHS,TJKSRQ,TJJSRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDateS','yyyyMMdd'),to_date('$statDateE','yyyyMMdd'))"""
        try {
          Class.forName(utils.cp.getProperty("driver"))
          conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
          ps = conn.prepareStatement(sql)
          conn.setAutoCommit(false)
          var iCnt: Int = 0
          row.foreach(data => {
            iCnt += 1
            OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 18, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 19, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 20, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 21, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 22, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 23, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 24, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 25, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 26, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 27, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 28, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 29, ps, java.sql.Types.VARCHAR)
            OracleUtils.ExecPS(data, 30, ps, java.sql.Types.NUMERIC)
            OracleUtils.ExecPS(data, 31, ps, java.sql.Types.NUMERIC)
            ps.addBatch()

            if (iCnt % 10000 == 0 && iCnt != 0) {
              println(s"============================$iCnt")
              ps.executeBatch()
              ps.clearBatch()
              conn.commit()
            }
          })

          println(s"============================$iCnt")
          ps.executeBatch()
          ps.clearBatch()
          conn.commit()

        } finally {
          if (ps != null) {
            ps.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      })
  }

  def linetotal(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (PI_LINE_NO,PI_ORG_NO,POWEROFF_CNT,PLAN_POWEROFF_CNT,PLAN_POWEROFF_DURATION,PLAN_LOSS_POWER,TEMP_POWEROFF_CNT,
        TEMP_POWEROFF_DURATION,TEMP_LOSS_POWER,PROD_POWEROFF_CNT,PROD_POWEROFF_DURATION,PROD_LOSS_POWER,POWEROFF_CIS_CONS_CNT,POWEROFF_PI_CONS_CNT,
        PI_CONS_CNT,CIS_CONS_CNT,INTEGRITY_RATE,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" + (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def linetotalW(data: DataFrame, tableName: String, statDateS: String, statDateE: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDateS','yyyyMMdd')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (PI_LINE_NO,PI_ORG_NO,POWEROFF_CNT,PLAN_POWEROFF_CNT,PLAN_POWEROFF_DURATION,PLAN_LOSS_POWER,TEMP_POWEROFF_CNT,
        TEMP_POWEROFF_DURATION,TEMP_LOSS_POWER,PROD_POWEROFF_CNT,PROD_POWEROFF_DURATION,PROD_LOSS_POWER,POWEROFF_CIS_CONS_CNT,POWEROFF_PI_CONS_CNT,
        PI_CONS_CNT,CIS_CONS_CNT,INTEGRITY_RATE,TJKSRQ,TJJSRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDateS','yyyyMMdd'),to_date('$statDateE','yyyyMMdd'))"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def aFixPreStat(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.distinct().foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (
                DWBM, ZYCFTD_PBSL, DYCFTD_YHSL, ZYJHTDCS,  ZYJHTDSC,
                ZYJHTDYXDL,  ZYJHTD_PBSL, ZYJHCFTD_PBSL, ZYGZTDCS,  ZYGZTDSC,
                ZYGZTDYXDL,  ZYGZTD_PBSL, ZYGZCFTD_PBSL, ZYLSJXTDCS,  ZYLSJXTDSC,
                ZYLSJXYXDL,  ZYLSJXTD_PBSL, ZYLSJXCFTD_PBSL, DYJHTDCS,  DYJHTDSC,
                DYJHTDYXDL,  DYJHTD_YHSL, DYJHCFTD_YHSL, DYLSJXTDCS,  DYLSJXTDSC,
                DYLSJXTDYXDL,  DYLSJXTD_YHSL, DYLSJXCFTD_YHSL, DYGZTDCS,  DYGZTDSC,
                DYGZTDYXDL,  DYGZTD_YHSL, DYGZCFTD_YHSL, CNW,  DWMC,
                DWJB,  SJDWBM,  SJDWMC,  GBTQSL,  ZBTQSL,TJRQ) 
        values(
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,""" + (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 18, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 19, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 20, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 21, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 22, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 23, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 24, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 25, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 26, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 27, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 28, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 29, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 30, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 31, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 32, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 33, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 34, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 35, ps, java.sql.Types.VARCHAR)
          
          OracleUtils.ExecPS(data, 36, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 37, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 38, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 39, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 40, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def aFixPreStatW(data: DataFrame, tableName: String, statDateS: String, statDateE: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDateS','yyyyMMdd')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.distinct().foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = 
        s"""insert into pwyw.$tableName (
                DWBM, ZYCFTD_PBSL, DYCFTD_YHSL, ZYJHTDCS,  ZYJHTDSC,
                ZYJHTDYXDL,  ZYJHTD_PBSL, ZYJHCFTD_PBSL, ZYGZTDCS,  ZYGZTDSC,
                ZYGZTDYXDL,  ZYGZTD_PBSL, ZYGZCFTD_PBSL, ZYLSJXTDCS,  ZYLSJXTDSC,
                ZYLSJXYXDL,  ZYLSJXTD_PBSL, ZYLSJXCFTD_PBSL, DYJHTDCS,  DYJHTDSC,
                DYJHTDYXDL,  DYJHTD_YHSL, DYJHCFTD_YHSL, DYLSJXTDCS,  DYLSJXTDSC,
                DYLSJXTDYXDL,  DYLSJXTD_YHSL, DYLSJXCFTD_YHSL, DYGZTDCS,  DYGZTDSC,
                DYGZTDYXDL,  DYGZTD_YHSL, DYGZCFTD_YHSL, CNW,  DWMC,
                DWJB,  SJDWBM,  SJDWMC,  GBTQSL,  ZBTQSL,TJKSRQ,TJJSRQ)
        values(
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,
        ?,?,?,?,?,to_date('$statDateS','yyyyMMdd'),to_date('$statDateE','yyyyMMdd'))"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 18, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 19, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 20, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 21, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 22, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 23, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 24, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 25, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 26, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 27, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 28, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 29, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 30, ps, java.sql.Types.NUMERIC)
          
          OracleUtils.ExecPS(data, 31, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 32, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 33, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 34, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 35, ps, java.sql.Types.VARCHAR)
          
          OracleUtils.ExecPS(data, 36, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 37, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 38, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 39, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 40, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def aFixLineStat(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (DWBM,KXJHTDCS,KXGZTDSC,KXLSJHTDSC,KXJHTDSC,KXGZTDCS,KXLSJHTDCS,SLCFTDCSJH,SLCFTDCSLSJH,SLCFTDCSGZ,
        DWMC,DWJB,SJDWBM,SJDWMC,XLSL,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" + (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def aFixLineStatW(data: DataFrame, tableName: String, statDateS: String, statDateE: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDateS','yyyyMMdd')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (DWBM,KXJHTDCS,KXGZTDSC,KXLSJHTDSC,KXJHTDSC,KXGZTDCS,KXLSJHTDCS,SLCFTDCSJH,SLCFTDCSLSJH,SLCFTDCSGZ,
        DWMC,DWJB,SJDWBM,SJDWMC,XLSL,TJKSRQ,TJJSRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDateS','yyyyMMdd'),to_date('$statDateE','yyyyMMdd'))"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }
  
}