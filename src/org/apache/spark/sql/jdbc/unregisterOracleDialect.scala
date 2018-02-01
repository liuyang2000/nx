package org.apache.spark.sql.jdbc

import java.sql.Types
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.MetadataBuilder

object unregisterOracleDialect {
  def unregister {
    val dialect = new JdbcDialect {

      override def canHandle(url: String): Boolean = {
        url.startsWith("jdbc:oracle")
      }

      override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
        //        if (sqlType == Types.NUMERIC) {
        //          val scale = if (null != md) md.build().getLong("scale") else 0L
        //          size match {
        //            case 0                                 => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
        //            case _ if scale == -127L               => Option(DecimalType(DecimalType.MAX_PRECISION, 10))
        //            case _ if scale > 0L                   => Option(FloatType)
        //            case _ if (size <= 9L && scale == 0L)  => Option(IntegerType)
        //            case _ if (size > 9L && scale == 0L) => Option(LongType)
        //            case _                                 => None
        //          }
        //        } else 
        if (sqlType == Types.DATE && typeName.equals("DATE") && size == 0)
          Option(org.apache.spark.sql.types.TimestampType)
        //                  Option(org.apache.spark.sql.types.StringType)
        else
          None
      }
    }

    JdbcDialects.unregisterDialect(OracleDialect)
    JdbcDialects.registerDialect(dialect)
  }
}