package Data

/**
 * Created by damien on 8/19/2015.
 */

import Util.{CreateSchema, CreateSparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

class DeptData {
  private val deptData = "department.csv"

  private def createSchema : StructType ={
    val cs = new CreateSchema
    cs.addField("deptId",IntegerType,false)
    cs.addField("department",StringType,false)
    cs.getSchema
  }

  private def makeRow = (x : Array[String]) => {
    Row(x(0).toInt,
        x(1))
  }

  def getData (sc : SparkContext) : DataFrame = {
    val deptRdd = sc.textFile(deptData).map(_.split(",")).map(makeRow)
    val schema = createSchema
    CreateSparkContext.getHiveContext.createDataFrame(deptRdd,schema)
  }
}
