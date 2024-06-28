import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CSVTransformations {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder.appName("CSV Transformations").getOrCreate()
    import spark.implicits._

    // Read CSV File
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("/tmp/catbd125/Tekle/student.csv")

    // Show DataFrame Schema
    df.printSchema()

    // Show First 5 Rows
    df.show(5)

    // Filter Rows Where Country is 'Cyprus'
    val dfCyprus = df.filter($"country" === "Cyprus")
    dfCyprus.show()

    // Select Specific Columns
    val dfSelect = df.select("id", "firstname", "lastname")
    dfSelect.show()

    // Add a New Column with Constant Value
    val dfWithConstant = df.withColumn("new_column", lit("constant_value"))
    dfWithConstant.show()

    // Rename a Column
    val dfRenamed = df.withColumnRenamed("phonenumber", "phone_number")
    dfRenamed.show()

    // Drop a Column
    val dfDropped = df.drop("country")
    dfDropped.show()

    // Group By Country and Count
    val dfGrouped = df.groupBy("country").count()
    dfGrouped.show()

    // Order By First Name
    val dfOrdered = df.orderBy("firstname")
    dfOrdered.show()

    // Filter Rows Where Phone Number is Greater Than 50000000
    val dfFiltered = df.filter($"phonenumber" > 50000000)
    dfFiltered.show()

    // Convert First Name to Upper Case
    val dfUpper = df.withColumn("firstname_upper", upper($"firstname"))
    dfUpper.show()

    // Concatenate First Name and Last Name
    val dfConcat = df.withColumn("full_name", concat($"firstname", lit(" "), $"lastname"))
    dfConcat.show()

    // Remove Duplicate Rows
    val dfDistinct = df.dropDuplicates()
    dfDistinct.show()

    // Calculate Length of First Name
    val dfLength = df.withColumn("firstname_length", length($"firstname"))
    dfLength.show()

    // Replace Null Values in a Column
    val dfFilled = df.na.fill(Map("phonenumber" -> 0))
    dfFilled.show()

    // Filter Rows Based on Length of Last Name
    val dfFilteredLength = df.filter(length($"lastname") > 5)
    dfFilteredLength.show()

    // Add a Column with Current Date
    val dfWithDate = df.withColumn("current_date", current_date())
    dfWithDate.show()

    // Aggregate by Country with Sum of Phone Numbers
    val dfAgg = df.groupBy("country").agg(sum("phonenumber").alias("total_phone_numbers"))
    dfAgg.show()

    // Join with Another DataFrame
    val data = Seq((1, "New York"), (2, "Beirut"), (3, "Cape Town"))
    val df2 = data.toDF("id", "city")

    val dfJoined = df.join(df2, Seq("id"), "inner")
    dfJoined.show()

    // Stop Spark Session
    spark.stop()
  }
}