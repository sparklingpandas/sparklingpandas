/*
 * These are additional aggregation UDFs we want to make available that are not available in standard Spark SQL
 */
package com.sparklingpandas;

import org.apache.spark.sql.EvilSqlTools
import org.apache.spark.sql.types.{DoubleType, NumericType}

object Utils {
  // This function seems shaaady
  // TODO: Do something more reasonable
  def toDouble(x: Any): Double = {
    x match {
      case x: NumericType => EvilSqlTools.toDouble(x.asInstanceOf[NumericType])
      case x: Long => x.toDouble
      case x: Int => x.toDouble
      case x: Double => x
    }
  }
}
