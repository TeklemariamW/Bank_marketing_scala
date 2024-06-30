import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Increamentalload {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("MiniPrjScala")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "bank")
      .option("driver", "org.postgresql.Driver")
      .option("user", "consultants")
      .option("password", "WelcomeItc@2022").load()

    df.printSchema()
    df.show()

    // read and show the existing_data in hive table
    val existing_hive_data = spark.read.table("tekle.bank_marketing_scala")
    existing_hive_data.show(5)

    // 4. Determine the incremental data
    /*
   "left_anti" specifies the type of join to perform.
    A left_anti join returns only the rows from the left DataFrame (df_upper in this case)
    that do not have corresponding matches in the right DataFrame (existing_hive_data).
    Essentially, it finds rows in df_upper that are not present in existing_hive_data based
    on id.
     */
    val incremental_data_df = df_upper.join(existing_hive_data, Seq("id"), "left_anti")
    print('------------------Incremental data-----------------------')
    incremental_data_df.show()

    //counting the number of the new records added to postgres tables
    val new_records = incremental_data_df.count()
    print('------------------COUNTING INCREMENT RECORDS ------------')
    print('new records added count', new_records)

    // 5.  Adding the incremental_data DataFrame to the existing hive table
    // Check if there are extra rows in PostgresSQL. if exist => # write & append to the Hive table
    if (incremental_data_df.count() > 0) {
      // Append new rows to Hive table
      incremental_data_df.write.mode("Append").saveAsTable("tekle.bank_marketing_scala")

    }
    else{
        print("No new records been inserted in PostgresSQL table.")
      }
    }
   //Read again from hive to see the updated data
    val updated_hive_data = spark.read.table("tekle.bank_marketing_scala")
    val df_ordered = updated_hive_data.orderBy(col("id").desc())
    df_ordered.show(5)

}