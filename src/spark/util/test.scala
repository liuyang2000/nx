package spark.util

import java.util.Calendar
import java.text.SimpleDateFormat

object test extends App {
//    val date1 = utils.sdfDay.parse("20170101")
//    val date2 = utils.sdfDay.parse("20170315")
//  
//    while (!date1.equals(date2)) {
//      println(s"""spark-submit --master yarn --conf "spark.memory.storageFraction=0.15" --driver-memory 2G --executor-memory 2G --executor-cores 4 --num-executors 2 --class spark.task.NX.submitTask /home/xtgsadmin/workspace/nx/target/nx-1.0-jar-all.jar GDTJ batch ${utils.sdfDay.format(date1)} DAY""")
//      date1.setTime(date1.getTime + 24 * 3600 * 1000l)
//    }
  

//  for (i <- 1 to 96) {
//    print(s"I$i,")
//  }
//  println("")
//  for (i <- 1 to 96) {
//    print(s"U$i,")
//  }
//  println("")
//  for (i <- 1 to 96) {
//    print(s"P$i,")
////  }
  val a = System.currentTimeMillis()/1000/3600
  val b = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2017-04-23 14:53:17")
  println((b.getTime/1000/3600 + 8) % 24)
//  val date = utils.sdfDay.parse("20170301")
//  val calendar = Calendar.getInstance
//        calendar.setTime(date)
//        calendar.add(Calendar.MONTH, -1)
//        println(utils.sdfDay.format(calendar.getTime))
}