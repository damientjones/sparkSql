/**
 * Created by damien on 8/19/2015.
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

class getEmpData {
  private val empData = "employee.csv"

  private def createSchema : StructType ={
    val cs = new CreateSchema
    cs.addField("firstName",StringType,false)
    cs.addField("lastName",StringType,false)
    cs.addField("deptId",IntegerType,false)
    cs.addField("salary",IntegerType,false)
    cs.getSchema
  }

  private def makeRow = (x : Array[String]) => {
    Row(x(0),
        x(1),
        x(2).toInt,
        x(3).toInt)
  }

  def getData (sc : SparkContext) : DataFrame = {
    val empRdd = sc.textFile(empData).map(_.split(",")).map(makeRow)
    val schema = createSchema
    createSparkContext.getHiveContext.createDataFrame(empRdd,schema)
  }
}
