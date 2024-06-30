import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object IncrementalLoad {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("IncrementalLoad")
      .config("spark.master", "local[*]")  // Adjust as per your deployment environment
      .enableHiveSupport()
      .getOrCreate()

    try {
      // Read data from PostgreSQL table
      val df = spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
        .option("dbtable", "bank")
        .option("driver", "org.postgresql.Driver")
        .option("user", "consultants")
        .option("password", "WelcomeItc@2022")
        .load()

      // Print schema and sample data from PostgreSQL
      df.printSchema()
      df.show()

      // Read existing data from Hive table
      val existing_hive_data = spark.read.table("tekle.bank_marketing_scala")
      existing_hive_data.show(5)

      // Determine incremental data using left_anti join
      val incremental_data_df = df.join(existing_hive_data, Seq("id"), "left_anti")
      println("------------------Incremental data-----------------------")
      incremental_data_df.show()

      // Count new records added to PostgreSQL table
      val new_records = incremental_data_df.count()
      println("------------------COUNTING INCREMENT RECORDS ------------")
      println(s"New records added count: $new_records")

      // Append incremental_data_df to the existing Hive table if there are new records
      if (new_records > 0) {
        incremental_data_df.write.mode("append").saveAsTable("tekle.bank_marketing_scala")
        println("New records appended to Hive table.")
      } else {
        println("No new records appended to Hive table.")
      }

      // Read updated data from Hive table and display ordered by id descending
      val updated_hive_data = spark.read.table("tekle.bank_marketing_scala")
      val df_ordered = updated_hive_data.orderBy(col("id").desc())
      println("Updated Hive table:")
      df_ordered.show(5)
    } finally {
      // Stop SparkSession at the end
      spark.stop()
    }
  }
}
