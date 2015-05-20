/*
 * These are additional aggregation UDFs we want to make available that are not available in standard Spark SQL
 */
package com.sparklingpandas;

import scala.collection.JavaConversions._

import scala.math._

import org.apache.commons.math.stat.StatUtils
import org.apache.commons.math.stat.descriptive.moment.{ Kurtosis => ApacheKurtosis}
import org.apache.spark.sql.Column
import org.apache.spark.sql.EvilSqlTools
import org.apache.spark.sql.types.{DoubleType, NumericType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.errors.TreeNodeException

object functions {
  def kurtosis(e: Column): Column = new Column(Kurtosis(EvilSqlTools.getExpr(e)))
}

case class Kurtosis(child: Expression) extends AggregateExpression {
  def this() = this(null)

  override def children = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def toString: String = s"Kurtosis($child)"
  override def newInstance() =  new KurtosisFunction(child, this)
}

case class KurtosisFunction(child: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null)

  var data = scala.collection.mutable.ArrayBuffer.empty[Any]
  override def update(input: Row): Unit = {
    data += child.eval(input)
  }

  // This function seems shaaady
  // TODO: Do something more reasonable
  private def toDouble(x: Any): Double = {
    x match {
      case x: NumericType => EvilSqlTools.toDouble(x.asInstanceOf[NumericType])
      case x: Long => x.toDouble
      case x: Int => x.toDouble
      case x: Double => x
    }
  }
  override def eval(input: Row): Any = {
    if (data.isEmpty) {
      println("No data???")
      null
    } else {
      val inputAsDoubles = data.toList.map(toDouble)
      println("computing on input "+inputAsDoubles)
      val inputArray = inputAsDoubles.toArray
      val apacheKurtosis = new ApacheKurtosis()
      val result = apacheKurtosis.evaluate(inputArray, 0, inputArray.size)
      println("result "+result)
      Cast(Literal(result), DoubleType).eval(null)
    }
  }
}
