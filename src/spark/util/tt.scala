package spark.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class A(id: Int, name: String)  
case class B(age : Int, id: Int, addr : String, name: String)  
object tt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("colRowDataFrame"). setMaster("local[2]")   
    val sc = new SparkContext(conf)  
    val sqlContext = new SQLContext(sc)   
    
    val ea = List(A(1, "iteblog"), A(2, "Jason"), A(3, "Abhi"))  
    val eb = List(B(1,2,"add1", "iteblog"), B(2,3,"add2", "iteblog"), B(3,4,"add3", "iteblog"))  

    val dfa = sqlContext.createDataFrame(ea)  
    val dfb = sqlContext.createDataFrame(eb)  
    dfa.join(dfb,Seq("id","name"),"outer").printSchema  
    
  }
}