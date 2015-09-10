package Data

/**
 * Created by damien on 8/19/2015.
 * Class extends CreateSchema trait
 */

import Util.{DataCheck, CreateSchema, CreateSparkContext, DateFormatter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DateType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

class EmpData (sc:SparkContext) extends CreateSchema {
  //Create fields and schema for employee in constructor
  private val empData = "employee.csv"
  addField("empId",IntegerType,false)
  addField("managerId",IntegerType,true)
  addField("firstName",StringType,false)
  addField("lastName",StringType,false)
  addField("deptId",IntegerType,false)
  addField("salary",IntegerType,false)
  addField("hireDate",DateType,false)
  private val schema = getSchema

  private def makeRow = (x : Array[String]) => {
    Row(x(0).toInt,
        DataCheck.checkInt(x(1)),
        x(2),
        x(3),
        x(4).toInt,
        x(5).toInt,
        DateFormatter.formatDate(x(6)))
  }

  def getData : DataFrame = {
    val empRdd = sc.textFile(empData).map(_.split(",")).map(makeRow)
    CreateSparkContext.getHiveContext.createDataFrame(empRdd,schema)
  }
}
