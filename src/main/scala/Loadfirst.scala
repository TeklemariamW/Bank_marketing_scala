
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Loadfirst {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("MiniPrjScala").enableHiveSupport().getOrCreate()
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb")
      .option("dbtable", "bank")
      .option("driver", "org.postgresql.Driver")
      .option("user", "consultants")
      .option("password", "WelcomeItc@2022").load()

    df.printSchema()
    df.show(10)

    val dfUpper = df.withColumn("job_upper", upper($"job"))
    dfUpper.show(5)

  }

}

// mvn package
//spark-submit --master local --jars /var/lib/jenkins/workspace/nagaranipysparkdryrun/lib/postgresql-42.5.3.jar --class scala_jenkins.loadPostgrestoHive target/MiniPrjScala-1.0-SNAPSHOT.jar

