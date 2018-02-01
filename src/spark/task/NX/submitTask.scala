package spark.task.NX

import java.util.Date
import spark.moudel.ATgDetailStatDRow
import spark.util.utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import spark.moudel.ALinesegTotalStatDProcessRow

object submitTask {
  def main(args: Array[String]) {
    org.apache.spark.sql.jdbc.unregisterOracleDialect.unregister
    args(0) match {
      case "GDDetail"    => GDDetail(args.tail)
      case "GDTJ"    => GDTJ(args.tail)
      case "PDZB"    => PDZB(args.tail)
      case "PDZB_GW" => PDZB_GW(args.tail)
      case "TYDB"    => TYDB(args.tail)
      case _         => println("WRONG PARAMETER")
    }
  }

  def GDDetail(args: Array[String]) {
    val name = "GDDetail"
    val sparkConf = new SparkConf().setAppName(name)
      .set("spark.sql.shuffle.partitions", "8")
      //    .set("spark.yarn.executor.memoryOverhead","600")
      .set("spark.sql.autoBroadcastJoinThreshold", "0") //禁止自动广播
      .set("spark.shuffle.reduceLocality.enabled", "false")
    //      .set("spark.kryoserializer.buffer.max", "1024m")
    //    logConf.setLogLevels
    //    sparkConf.registerKryoClasses(Array(classOf[ATmnlOnoffStatDProcessRow], classOf[ATmnlOnoffStatDRow]))
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    import hc.implicits._

    args(0) match {
      case "batch" => batch(args(1), args(2))
      case "oozie" => oozie("DAY")
    }

    def batch(start: String, statType: String) {
      val startDate = utils.sdfDay.parse(start)

      val GDDetail = new GDDetail(hc, startDate)
      GDDetail.execute("DAY")

    }

    def oozie(statType: String) {
      val startDate = utils.sdfDay.format(new Date)
      batch(startDate, "DAY")
    }
  }
  
  def GDTJ(args: Array[String]) {
    val name = "GDTJ"
    val sparkConf = new SparkConf().setAppName(name)
      .set("spark.sql.shuffle.partitions", "8")
      //    .set("spark.yarn.executor.memoryOverhead","600")
      .set("spark.sql.autoBroadcastJoinThreshold", "0") //禁止自动广播
      .set("spark.shuffle.reduceLocality.enabled", "false")
    //      .set("spark.kryoserializer.buffer.max", "1024m")
    //    logConf.setLogLevels
    //    sparkConf.registerKryoClasses(Array(classOf[ATmnlOnoffStatDProcessRow], classOf[ATmnlOnoffStatDRow]))
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    import hc.implicits._

    args(0) match {
      case "batch" => batch(args(1), args(2))
      case "oozie" => oozie(args(1))
    }

    def batch(start: String, statType: String) {
      val startDate = utils.sdfDay.parse(start)

      val GDTJ = new GDTJ(hc, startDate)
      GDTJ.execute(statType)

    }

    def oozie(statType: String) {
      val startDate = utils.sdfDay.format(new Date)
      batch(startDate, statType)
    }
  }

  def PDZB(args: Array[String]) {
    val name = "PDZB"
    val sparkConf = new SparkConf().setAppName(name)
      .set("spark.sql.shuffle.partitions", "8")
      //    .set("spark.yarn.executor.memoryOverhead","600")
      .set("spark.sql.autoBroadcastJoinThreshold", "0") //禁止自动广播
      .set("spark.shuffle.reduceLocality.enabled", "false")
    //      .set("spark.kryoserializer.buffer.max", "1024m")
    //    logConf.setLogLevels
    //    sparkConf.registerKryoClasses(Array(classOf[ATmnlOnoffStatDProcessRow], classOf[ATmnlOnoffStatDRow]))
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    import hc.implicits._

    args(0) match {
      case "batch" => batch(args(1), args(2))
      case "oozie" => oozie(args(1))
    }

    def batch(start: String, statType: String) {
      val startDate = utils.sdfDay.parse(start)

      val PDZB = new PDZB(hc, startDate)
      PDZB.execute(statType)

    }

    def oozie(statType: String) {
      val startDate = utils.sdfDay.format(new Date)
      batch(startDate, statType)
    }
  }

  def PDZB_GW(args: Array[String]) {
    val name = "PDZB_GW"
    val sparkConf = new SparkConf().setAppName(name)
      .set("spark.sql.shuffle.partitions", "8")
      //    .set("spark.yarn.executor.memoryOverhead","600")
      .set("spark.sql.autoBroadcastJoinThreshold", "0") //禁止自动广播
      .set("spark.shuffle.reduceLocality.enabled", "false")
    //      .set("spark.kryoserializer.buffer.max", "1024m")
    //    logConf.setLogLevels
    //    sparkConf.registerKryoClasses(Array(classOf[ATmnlOnoffStatDProcessRow], classOf[ATmnlOnoffStatDRow]))
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    import hc.implicits._

    args(0) match {
      case "batch" => batch(args(1), args(2))
      case "oozie" => oozie(args(1))
    }

    def batch(start: String, statType: String) {
      val startDate = utils.sdfDay.parse(start)

      val PDZB = new PDZB_GW(hc, startDate)
      PDZB.execute(statType)

    }

    def oozie(statType: String) {
      val startDate = utils.sdfDay.format(new Date)
      batch(startDate, statType)
    }
  }

  def TYDB(args: Array[String]) {
    val name = "TYDB"
    val sparkConf = new SparkConf().setAppName(name)
      .set("spark.sql.shuffle.partitions", "8")
      //    .set("spark.yarn.executor.memoryOverhead","600")
      .set("spark.sql.autoBroadcastJoinThreshold", "0") //禁止自动广播
      .set("spark.shuffle.reduceLocality.enabled", "false")
    //      .set("spark.kryoserializer.buffer.max", "1024m")
    //    logConf.setLogLevels
    //    sparkConf.registerKryoClasses(Array(classOf[ATmnlOnoffStatDProcessRow], classOf[ATmnlOnoffStatDRow]))
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    import hc.implicits._

    val date = if (args.size == 1) utils.sdfDay.parse(args(0)) else new Date((new Date).getTime - 2 * 24 * 3600 * 1000l)
    val TYDB = new TYDB(hc, date)
    NXToOracle.tydb(TYDB.statProv
      .unionAll(TYDB.statCity)
      .unionAll(TYDB.statTown), "PWYW_TYDBZB_SSX_R", utils.sdfDay.format(date), true)
  }
}