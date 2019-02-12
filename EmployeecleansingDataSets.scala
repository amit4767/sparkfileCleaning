package com.sparkTutorial.rdd
package com.sparkTutorial.rdd


import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._

/** Find the movies with the most ratings. */
object EmployeecleansingDataSets {
  

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("EmployeeSalary")
      .master("local[3]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val lines = spark.sparkContext.textFile("emp.txt")

    val headerColumns = lines.first().split(",").to[List]

    def dfSchema(columnNames: List[String]): StructType = {
      StructType(
        Seq(
          StructField(name = "name", dataType = StringType, nullable = false),
          StructField(name = "age", dataType = IntegerType, nullable = false),
          StructField(name = "salary", dataType = DoubleType, nullable = false),
          StructField(name = "benefits", dataType = IntegerType, nullable = false),
          StructField(name = "department", dataType = StringType, nullable = false)

        )
      )
    }

    final case class Employee(name:String,age:Int,salary:Double,benefits:Int,department:String)

    def row(line: List[String]): Row = {

      Row(line(0).replaceAll("\"","").trim,
         if(line(1).replaceAll("\"","").trim != "") line(1).replaceAll("\"","").trim.toInt else 0,
        if(line(2).replaceAll("\"","").trim != "") line(2).replaceAll("\"","").trim.toDouble else 0,
        if(line(3).replaceAll("\"","").trim != "") line(3).replaceAll("\"","").trim.toInt else 0,
        line(4).replaceAll("\"","").trim)

    }



    import scala.util.Try
    def isInt(aString: String): Boolean = Try(aString.toInt).isSuccess
    def isDouble(aString: String): Boolean = Try(aString.toDouble).isSuccess

    def mapError(line: List[String]) ={

      if(line.size == 5) {
        try {

          val x = if(!"".equals(line(1).replaceAll("\"","").trim)) {
            if (isInt(line(1).replaceAll("\"", "").trim))
              ""
            else ",age"
          }else ""


          val y = if(!"".equals(line(2).replaceAll("\"","").trim)) {

            if (isDouble(line(2).replaceAll("\"", "").trim)){
               ""}
            else if(x != "")
                {
                   ",salary"}
             else { "salary" }
          }else ""


          val z = if(!"".equals(line(3).replaceAll("\"","").trim)) {
            if (isInt(line(3).replaceAll("\"", "").trim))
              ""
            else if(y != "" || x!= "") ",benefits" else "benefits"
          }else ""


         if(x != "" || y!="" || z !="")
          ("The datatypes of columns:["+x+y+z+"]" ,line)
          else
           ("" ,line)

        }catch {
          case e: Exception => e.printStackTrace()
            ("some unknown error" ,line)
        }




      }else {
        ("The number of columns in the record doesn't match file header spec." , line)
      }
    }

    val schema = dfSchema(headerColumns)
    //Cast all the columns to its type from the list

    val data2 = lines
        .mapPartitionsWithIndex((index, element) => { if (index == 0  )  element.drop(2)  else element}) // skip header
        .map(_.split(",").to[List]).map(mapError)


    val errorrow  = data2.filter(x => x._1 !="")

    errorrow.coalesce(1)
      .saveAsTextFile("quar")
    val goodrow  = data2.filter(x => x._1 =="")


val  data = goodrow.map(x => x._2).map(row)



     // val data  =


    val dataFrame = spark.createDataFrame( data, schema)


    dataFrame.printSchema()

    dataFrame.show()
    // Stop the session
    spark.stop()
  }
  
}

