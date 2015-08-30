/**
 * Created by damien on 8/29/2015.
 */
import org.apache.spark.sql.types.{StructField, StructType,DataType}
import scala.collection.mutable.ArrayBuffer
class CreateSchema {
  private var schema = ArrayBuffer.empty[StructField]

  def addField (fieldName:String,fieldType:DataType,nullable:Boolean) {
    schema += StructField(fieldName,fieldType,nullable)
  }

  def getSchema : StructType = {
    StructType(schema)
  }
}
