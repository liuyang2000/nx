package spark.util

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.DataFrame
import java.lang.management.ManagementFactory
import org.apache.spark.SparkEnv

object utils {
  val sdfDay = new SimpleDateFormat("yyyyMMdd");
  val sdfDayOracle = new SimpleDateFormat("yyyy-MM-dd");
  val sdfOracleDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val sdfHour = new SimpleDateFormat("yyyyMMddHH");
  val sdfSec = new SimpleDateFormat("yyyyMMddHHmmss");
  val sdfMonth = new SimpleDateFormat("yyyyMM");
  val sdfYear = new SimpleDateFormat("yyyy");
  val ENERGY_LOAD_DAYS = 30;

  def calcDt(time: Long) = (time + 8 * 1000 * 3600) / (3600 * 1000 * 24) % 2

  def calcDayStamp(time: Long) = (time + 8 * 1000 * 3600) / (3600 * 1000 * 24)

  //	private static  String TABLE_NAME = "sea.E_MP_DAY_READ_VER_TST";
  val FIX_PAP = "PAP_R";

  val FIX_SUCCESS = "1";
  val FIX_FAIL = "-1";
  val POWER_PUB_K = 1; //居民、低压非居
  val POWER_PRI_K = 3; //其他
  val P_MAX = 60 * 220 * 24 * POWER_PUB_K / 1000;
  //	private static long E_MP_DAY_READ_INSERT_INTERVAL = 5*60*1000;

  val area_code = for (i <- (1 to 17).toArray if i != 11)
    yield if (i < 10) s"AREA_CODE = '0$i'"
  else s"AREA_CODE = '$i'"

  val cutFlag: String = ":"
  val orgPartArr: Array[String] = Array("34401:3440101", "3440101:3440102", "3440102:3440103", "3440103:3440104", "3440104:34402", "34402:3440201", "3440201:3440202", "3440202:3440203", "3440203:34403", "34403:3440301", "3440301:34404", "34404:3440401", "3440401:3440402", "3440402:3440403", "3440403:3440404", "3440404:34405", "34405:3440501", "3440501:3440502", "3440502:3440503", "3440503:34406", "34406:3440601", "3440601:34407", "34407:3440701", "3440701:34408", "34408:3440801", "3440801:3440802", "3440802:3440803", "3440803:3440804", "3440804:3440805", "3440805:3440806", "3440806:3440807", "3440807:3440808", "3440808:34409", "34409:3440901", "3440901:3440902", "3440902:3440903", "3440903:3440904", "3440904:3440905", "3440905:3440906", "3440906:34410", "34410:3441001", "3441001:3441002", "3441002:3441003", "3441003:3441004", "3441004:3441005", "3441005:3441006", "3441006:3441007", "3441007:3441101", "3441101:3441102", "3441102:3441103", "3441103:3441104", "3441104:34412", "34412:3441201", "3441201:3441202", "3441202:3441203", "3441203:3441204", "3441204:3441205", "3441205:34413", "34413:3441301", "3441301:3441302", "3441302:3441303", "3441303:3441304", "3441304:3441305", "3441305:3441306", "3441306:34414", "34414:3441401", "3441401:3441402", "3441402:3441403", "3441403:3441404", "3441404:3441405", "3441405:3441406", "3441406:34415", "34415:3441501", "3441501:3441502", "3441502:3441503", "3441503:3441504", "3441504:3441505", "3441505:3441506", "3441506:34416", "34416:3441601", "3441601:3441602", "3441602:3441603", "3441603:3441604", "3441604:34417", "34417:3441701", "3441701:3441702", "3441702:3441703", "3441703:34418")
  val partBArray = for (i <- orgPartArr)
    yield s"ORG_NO >= '${i.split(cutFlag)(0)}' AND ORG_NO < '${i.split(cutFlag)(1)}'"
  val cp = new Properties
  cp.put("user", "pwyw")
  cp.put("password", "nxpms_pwyw!")
  cp.put("driver", "oracle.jdbc.driver.OracleDriver")
  val url113 = "jdbc:oracle:thin:@//10.217.14.203:11521/sgpms"

    val cptest = new Properties
  cptest.put("user", "pwyw")
  cptest.put("password", "pwyw")
  cptest.put("driver", "oracle.jdbc.driver.OracleDriver")
  val urltest = "jdbc:oracle:thin:@//10.217.14.95:11521/orcl"
  
  val urlSGPMS = "jdbc:oracle:thin:@//10.217.96.52:1521/o2000"
  val cpSGPMS = new Properties
  cpSGPMS.put("user", "ems")
  cpSGPMS.put("password", "naritech")
  cpSGPMS.put("driver", "oracle.jdbc.driver.OracleDriver")

  val hbaseConfig = new Configuration
  hbaseConfig.set("dfs.nameservices", "nameservice1")
  hbaseConfig.set("dfs.ha.namenodes.nameservice1", "namenode688,namenode844")
  hbaseConfig.set("dfs.namenode.rpc-address.nameservice1.namenode688", "hadoop601.nari.com:8020")
  hbaseConfig.set("dfs.namenode.rpc-address.nameservice1.namenode844", "hadoop602.nari.com:8020")

  hbaseConfig.set("hbase.rootdir", "hdfs://nameservice1/hbase")
  hbaseConfig.set("hbase.zookeeper.quorum", "hadoop607.nari.com,hadoop603.nari.com,hadoop611.nari.com,hadoop609.nari.com,hadoop605.nari.com")

  val v_province_no = "34101"

  def getPreDay = {
    val calendar = Calendar.getInstance
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.getTime
  }

  def tryCatch(table: String)(block: => Unit) {
    try {
      block
    } catch {
      case e: Exception => {
        e.printStackTrace()
        println(s"==================write $table fail========================")
      }
    }
  }

  def printPartition(df: DataFrame)(name: String) = {
    println(s"+++++++++++++++++++++++$name++++++++++++++++++++++++++++++++++++++++")
    df.mapPartitions { r => Iterator[(String, Long)]((SparkEnv.get.executorId, r.size)) }.groupBy(r => r._1).collect.map({
      case (id, arry) => s"$id:  (${arry.map(_._2).mkString(",")})"
    }).foreach(println)

  }

}