package Data

/**
 * Created by damien on 8/19/2015.
 * Class extends uses create schema trait
 */

import Util.{CreateSchema, CreateSparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

class DeptData (sc:SparkContext) extends CreateSchema {
  //Create fields and schema for department in constructor
  private val deptData = "department.csv"
  addField("deptId",IntegerType,false)
  addField("department",StringType,false)
  private val schema = getSchema

  private def makeRow = (x : Array[String]) => {
    Row(x(0).toInt,
        x(1))
  }

  def getData : DataFrame = {
    val deptRdd = sc.textFile(deptData).map(_.split(",")).map(makeRow)
    CreateSparkContext.getHiveContext.createDataFrame(deptRdd,schema)
  }
}
