package Util

/**
 * Created by damien on 8/29/2015.
 * Created as a trait so that any class can use it
 */
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import scala.collection.mutable.ArrayBuffer

trait CreateSchema {
  private var schema = ArrayBuffer.empty[StructField]

  protected def addField (fieldName:String,fieldType:DataType,nullable:Boolean) {
    schema += StructField(fieldName,fieldType,nullable)
  }

  protected def getSchema : StructType = {
    StructType(schema)
  }
}
