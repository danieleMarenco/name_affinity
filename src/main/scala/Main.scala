import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  implicit val system       = ActorSystem("name-affinity")
  implicit val materializer = ActorMaterializer()

  //columns
  val columnName          = "name"
  val count               = "count"
  val alphabet: Seq[Char] = 'A' to 'Z'

  val spark = SparkSession.builder().appName("Spark test").master("local[*]").getOrCreate()

  def countLetter(col: Column, letter: Char): Column = {
    // [^A] negazione
    length(expr(s"regexp_replace(${columnName}, '[^${letter}]', '')"))
  }

  def dataFrameWithAlphabetCount(nomi_iban: DataFrame) = {

    var dataFrameWithAlphabetCount: DataFrame = nomi_iban.withColumn("count", length(col(columnName)))

    alphabet.foreach(letter => {
      dataFrameWithAlphabetCount = dataFrameWithAlphabetCount.withColumn(letter.toString, countLetter(col(columnName), letter))
    })

    val concatenatedAlphabet = alphabet.mkString("", "+", "")
    dataFrameWithAlphabetCount = dataFrameWithAlphabetCount.withColumn("alphabet_count", expr(concatenatedAlphabet))
    dataFrameWithAlphabetCount =
      dataFrameWithAlphabetCount.withColumn("checksum_alphabet_count", expr(s"${"count"} == ${"alphabet_count"}"))

    dataFrameWithAlphabetCount
  }

  def readCsv(path: String): DataFrame = {
    val df = spark.read.format("csv").load(path).toDF(columnName: String).withColumn(columnName, upper(col(columnName)))
    println(path +" -> " + df.count())
    df
  }

  def start(): Unit = {
    val nomi_iban     = readCsv("nomi.csv")
    val nomi_italiani = readCsv("nomi_italiani.csv")

    val commonName: DataFrame = nomi_iban.join(nomi_italiani, columnName)
    println("common_name -> " + commonName.count())

    val affinity = dataFrameWithAlphabetCount(commonName)
    affinity.write.format("csv").save("affinity.csv")
    affinity.show(40)

    println("Shutting down...")
    Http().shutdownAllConnectionPools().foreach(_ => system.terminate)
  }

  def main(args: Array[String]): Unit = {
    Main.start()
  }
}