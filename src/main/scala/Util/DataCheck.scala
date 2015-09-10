package Util

/**
 * Created by damien on 9/9/2015.
 * Created as object so it is serializable
 */
object DataCheck {
  def checkString = (str:String) => {
     if (str.isEmpty){null.asInstanceOf[String]}else{str}
  }
  def checkInt = (str:String) => {
    if (str.isEmpty){null.asInstanceOf[Integer]}else{str.toInt}
  }
}
