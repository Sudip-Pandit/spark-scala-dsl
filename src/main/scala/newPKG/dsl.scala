package newPKG
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import shapeless.syntax.std.tuple.unitTupleOps
import spire.implicits.eqOps

object dsl {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DSL-APP").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    sc.setLogLevel("ERROR")

    println("=== Read the dataset from source path ===============")
    val df = spark.read.format("csv").option("header", "true")
      .load("file:///C:/Users/Sudip/Desktop/txns.csv")


    print("==============filter dataframe=========")
    val df1 = df.filter(col("category") === "Gymnastics")
    df1.show(5)


    println("=====filter multiple columns====")
    val df2 = df.filter(col("category") === "Gymnastics" || col("spendby") == "credit")
    df2.show(5)


    val df22 = df.filter("amount > 50").show(5)


    println("=====new column added====")
    val newclm = df.withColumn("new", lit("null"))
    newclm.show(6)


    println("====select some columns only=====")
    val somecols = df.select("txnno", "txndate", "txnno")
    somecols.show(5)


    println("=====change the column name inside the select statements=====")
    val col_renamed = df.withColumnRenamed("txnno", "txnno_new")
    col_renamed.show(5)


    print("====Select Expression======")
    /* SelectExpr is similar to select() with the only difference being
     it accepts SQL expressions (in string format) that will be executed*/


    val selexpr = df.selectExpr("spendby", "case when spendby == 'credit' then 1 else 0 end as not_qualified").show(5)


    print("===next selectExpr examples=====")

    val selExp1 = df.selectExpr("amount", "amount+10 as total_amount").show(5)

    val selExp2 = df.selectExpr("txnno", "txndate", "amount", "amount-30 as modified_amount", "category", ("case when category == 'Gymnastics' then 1 else 0 end as modified_games_types")).show(4)




  }
  }
