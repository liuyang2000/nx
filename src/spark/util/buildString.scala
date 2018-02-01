package spark.util

object buildstring extends App {

  val str = """
     |-- CONS_NO: string (nullable = true)
     |-- POWEROFF_CNT: integer (nullable = true)
     |-- PLAN_POWEROFF_CNT: integer (nullable = true)
     |-- PLAN_POWEROFF_DURATION: float (nullable = true)
     |-- PLAN_LOSS_POWER: float (nullable = true)
     |-- TEMP_POWEROFF_CNT: integer (nullable = true)
     |-- TEMP_POWEROFF_DURATION: float (nullable = true)
     |-- TEMP_LOSS_POWER: float (nullable = true)
     |-- PROD_POWEROFF_CNT: integer (nullable = true)
     |-- PROD_POWEROFF_DURATION: float (nullable = true)
     |-- PROD_LOSS_POWER: float (nullable = true)
     |-- CONS_NAME: string (nullable = true)
     |-- ELEC_ADDR: string (nullable = true)
     |-- YX_TQ_BS: long (nullable = true)
     |-- YX_TG_MC: string (nullable = true)
     |-- PMS_BYQ_BS: string (nullable = true)
     |-- PMS_BYQ_MC: string (nullable = true)
     |-- DQTZ: string (nullable = true)
     |-- BZID: string (nullable = true)
     |-- CNW: string (nullable = true)
     |-- PMS_DWBM: string (nullable = true)
     |-- PMS_DWMC: string (nullable = true)
     |-- PMS_DWCJ: string (nullable = true)
     |-- SJDWID: string (nullable = true)
     |-- SJDWMC: string (nullable = true)
"""

  def filte = str.replaceAll("val ", "").replaceAll(": String", "").replaceAll(": java.lang.Float", "").replaceAll(": java.lang.Long", "").replaceAll(": java.math.BigDecimal", "").replaceAll(": java.lang.Integer", "").replaceAll(": java.lang.Double", "").replaceAll("\\s+", "")

  def builtProcuct {
    val result = filte
    val a = result.split(",").zipWithIndex.map(r => "case " + r._2 + " => " + r._1)
    println(a.mkString("\n"))
  }

  def chose = {
    val pp = "val \\S+:".r
    for (str1 <- pp.findAllIn(str)) {
      val str = str1.substring(4, str1.length - 1)
      println(s"$str1 java.lang.Float = choseS(r._1.$str, r._2.$str)")
    }
  }

  //    println(str.split(",").map(r=>r.split(" ").apply(0)).mkString("\",\"").replaceAll("\\s+", ""))
  //chose
  //  println(filte.replaceAll(":DataFrame", "").split(",").map(r=>s"fun($r)").mkString(","))
  buildSchema3
  //  println(str.split("\n").map(r => r.split(" ")(1)).reduce((a,b) => s"""$a.join($b, Seq("PMS_DWBM"), "outer")"""))

  //    println(filte)
  //    buildHive
  // buildSchema4
  //  buildHive

  def buildSchema {
    println(str.replaceAll("\n", ""))
    //    val result = str.replaceAll("\\s+", ",").split(",").map("val " + _ + ": String").mkString(", ")
    //    println(result)
  }

  def buildSchema2 {
    val result = str.split("\\n").map("val " + _ + ": String").mkString(", ")
    println(result)
  }

  def buildAdd {
    println(filte.split(",").map(r => s"val $r = add(r._1.$r,r._2.$r)").mkString("\n"))
  }

  def buildHive {
    val str1 = str.replaceAll("\\|--", "").replaceAll(" long", "bigint").replaceAll("\\(nullable = false\\)", "").replaceAll("\\(nullable = true\\)", "").replaceAll(" ", "")
    val result = str1.split(":").mkString(" ")
    val arry = result.split("\n")
    println(arry.mkString(",\n"))
  }

  def buildCaseClass {
    val str1 = str.replaceAll("\\|--", "").replaceAll("\\(nullable = false\\)", "").replaceAll("\\(nullable = true\\)", "").replaceAll(" ", "")
    val result = str1.split(":").mkString(" ")
    val arry = result.split("\n")
    val result1 = arry.map {
      r =>
        {
          val row = r.split(" ")
          "val " + row(0) + ": java.lang." + row(1).charAt(0).toUpper + row(1).tail
        }
    }
    println(result1.mkString(",\n"))
  }

  def buildSchema3 {
    val str1 = str.replaceAll("\\|--", "").replaceAll("\\(nullable = false\\)", "").replaceAll("\\(nullable = true\\)", "").replaceAll(" ", "")
    val result = str1.split(":").mkString(" ")
    val arry = result.split("\n")
    val result1 = arry.zipWithIndex.map {
      r =>
        {
          val row = r._1.split(" ")
          if (row(1) == "string")
            s"""oracleUtils.ExecPS(data, ${r._2 + 1}, ps, java.sql.Types.VARCHAR)"""
          else if (row(1) == "timestamp")
            s"""oracleUtils.ExecPS(data, ${r._2 + 1}, ps, java.sql.Types.TIMESTAMP)"""
          else
            s"""oracleUtils.ExecPS(data, ${r._2 + 1}, ps, java.sql.Types.NUMERIC)"""
        }
    }
    println(arry.zipWithIndex.map(r => r._2 + " " + r._1).mkString("\n"))
    println(result1.mkString("\n"))
    println(arry.size)
    println((1 to arry.size).map(_ => "?").mkString(","))
    println(arry.map(_.replaceAll(" string", "").replaceAll(" float", "").replaceAll(" integer", "").replaceAll(" timestamp", "").replaceAll(" decimal(\\S+)", "").replaceAll(" long", "").replaceAll(" double", "")).mkString(","))
  }

  def buildSchema4 {
    val arry = str.split("\n")
    val result = arry.map { r =>
      {
        val a = r.split(" ")
        "val " + a(0) + ": " + a(1)
      }
    }.mkString(",\n")
    println(result)
  }
  
  //  println(str.replaceAll("\\s+", ","))
  //  builtProcuct
  //  val result = str.replaceAll("val ", "").replaceAll(": String", "").replaceAll(": java.lang.Float", "").replaceAll(": java.math.BigDecimal", "").replaceAll(": java.lang.Integer", "").replaceAll(": java.lang.Double", "").replaceAll("\\s+", "")
  //  println(result)
  //  val a = str.split(",").map("val " + _).mkString(",")
  //  println(a)
  //    println(result)

  def builtMember {
    val arry = str.replaceAll("\\s+", " ").split(" ")
    println((1 to arry.size).map(_ => "?").mkString(","))
    val result = arry.mkString(",")
    println(""""""" + result.replaceAll(",", """","""") + """"""")
    println(result)
    //    println(result.split(",").map("r._2."+_).mkString(","))
  }

  def built_E {
    val result = filte
    val result1 = result.split(",").map(_ + "_S").mkString(",")
    println(result1.replaceAll(",", """",""""))
  }

  //    println(2*24 * 3600 * 1000)
  //  val a = str.split(",").map(_ => "?").mkString(",")
  //  val a = str.replaceAll("\\s+","").split(",").mkString("""","""")
  //  println(a)
  //    val a = for(i<-1 to 96)
  //      yield "val Q"+i + ": java.lang.Float"
  //builtReadCase
  def builtReadCase {
    val result = filte.split(",").map(r => """col = cf1.getOrElse("""" + r + """", null)""")
    val result1 = str.replaceAll("  ", "").split(",").map(r => r + " = if(col == null) null else col.toFloat")
    for (i <- 0 until result.size) {
      println(result(i))
      println(result1(i))
    }
  }
}