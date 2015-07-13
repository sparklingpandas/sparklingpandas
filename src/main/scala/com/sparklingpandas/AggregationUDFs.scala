/*
 * These are additional aggregation UDFs we want to make available that are not available in standard Spark SQL
 */
package com.sparklingpandas;

import scala.collection.JavaConversions._

import scala.math._

import org.apache.commons.math.stat.StatUtils
import org.apache.commons.math.stat.descriptive.moment.{ Kurtosis => ApacheKurtosis}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.EvilSqlTools
import org.apache.spark.sql.types.{DoubleType, NumericType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.errors.TreeNodeException

// functions we want to be callable from python
object functions {
  def kurtosis(e: Column): Column = new Column(Kurtosis(EvilSqlTools.getExpr(e)))
  def histogram(e: DataFrame, buckets: Int): java.util.Map[String, Long] = helpers.dataFrameHistogram(e, buckets)
  def registerUdfs(sqlCtx: SQLContext): Unit = {
    sqlCtx.udf.register("rowKurtosis", helpers.rowKurtosis _)
  }
}

// helper functions, not meant to be called directly from Python
object helpers {
  def rowKurtosis(x: Any*): Double = {
    val inputAsDoubles = x.map(Utils.toDouble)
    val inputArray = inputAsDoubles.toArray
    val apacheKurtosis = new ApacheKurtosis()
    apacheKurtosis.evaluate(inputArray, 0, inputArray.size)
  }

  def dataFrameHistogram(e: DataFrame, buckets: Int):
      java.util.Map[String, Long] = {
    def rowHistogram(f: Row => Double) = {
      val indexCounts = e.map(f).histogram(buckets)
      val map = new scala.collection.mutable.HashMap[String, Long]()
      var index = 0
      while (index < indexCounts._2.size) {
        val name = indexCounts._1(index).toString+"-"+indexCounts._1(index+1)
        val counts = indexCounts._2(index)
        map += ((name, counts))
      }
      map
    }
    // Construct a histogram of the input. Currently uses only minimum magic, future
    // use more magic. Note: we ignore bucket #s for String inputs.
    val schema = e.schema
    val fields = schema.fields
    fields.head match {
      case StructField(_, StringType, _, _) => e.map(_.getString(0)).countByValue()
      case StructField(_, IntegerType, _, _) => rowHistogram(_.getInt(0).toDouble)
      case StructField(_, LongType, _, _) => rowHistogram(_.getLong(0).toDouble)
      case StructField(_, DoubleType, _, _) => rowHistogram(_.getDouble(0).toDouble)
      case _ => throw new Exception("Unsopported schema " + schema)
    }
  }

}

// Compute the kurtosis on columns
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

  override def eval(input: Row): Any = {
    if (data.isEmpty) {
      null
    } else {
      val inputAsDoubles = data.toList.map(Utils.toDouble)
      val inputArray = inputAsDoubles.toArray
      val apacheKurtosis = new ApacheKurtosis()
      val result = apacheKurtosis.evaluate(inputArray, 0, inputArray.size)
      Cast(Literal(result), DoubleType).eval(null)
    }
  }
}
