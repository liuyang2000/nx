package spark.util

trait parseString {
  @inline protected def toDec(col: String) = {
    try {
      if (col == null) null else new java.math.BigDecimal(col)
    } catch {
      case _: Throwable => null
    }
  }
  
  @inline protected def toFloat(col: String) = {
    try {
      if (col == null) null else new java.lang.Float(col)
    } catch {
      case _: Throwable => null
    }
  }
  
  @inline protected def toInt(col: String) = {
    try {
      if (col == null) null else new java.lang.Integer(col)
    } catch {
      case _: Throwable => null
    }
  }
  
  @inline protected def toLong(col: String) = {
    try {
      if (col == null) null else new java.lang.Long(col)
    } catch {
      case _: Throwable => null
    }
  }
  
  @inline protected def toDouble(col: String) = {
    try {
      if (col == null) null else new java.lang.Double(col)
    } catch {
      case _: Throwable => null
    }
  }
}