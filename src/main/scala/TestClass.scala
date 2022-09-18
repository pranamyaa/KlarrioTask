
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SparkSession, functions}

object TestClass {
  def main(args : Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val format_title = functions.udf(formatTitle(_: String))
    val format_writer = functions.udf(formatWriter(_: String))

    val df_input = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load("src/data/input_file.csv")
      .filter($"input_title".isNotNull)
      .withColumn("abb_string", format_title($"input_title"))
      .withColumn("input_writer_words",format_writer($"input_writers"))
      .limit(400)

    val df_lookupKey = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load("src/data/lookupKeyDB1.csv")
      .withColumn("lookup_key", functions.trim($"lookup_key"))
      .withColumn("database_song_code_string", $"database_song_code".cast(StringType))
      .drop($"database_song_code")

    val df_matchedData = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load("src/data/Matched400.csv")

    df_matchedData.show(false)

    val df_songCode = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load("src/data/SongCodeDB2.csv")

    val matching_data = df_input
      .join(df_lookupKey, $"lookup_key"===$"abb_string")
      .join(df_songCode, $"database_song_code"===$"database_song_code_string")
      .filter($"unique_input_record_identifier" === "1NsbdxXsMS3m8Q79P6GWLo")

      /*
      .groupBy($"unique_input_record_identifier", $"input_title", $"abb_string")
      .agg(
        functions.count("*").as("no_of_candidate_records")
      )
      .orderBy($"no_of_candidate_records".desc)
       */

    println("matched out of 400: "+matching_data.count())
    matching_data.show( false)

  }

  def formatWriter(w: String):List[String] ={
    var inputWriter = w
    inputWriter = inputWriter.toUpperCase
    val inputWriterList = inputWriter.split(" ").toList

    inputWriterList
  }

  def formatTitle(s: String): String = {
    var inputTitle = s
    inputTitle = formatStemming(inputTitle)
    inputTitle = StringUtils.substringBefore(inputTitle, "(")
    inputTitle = removeConsecutiveDuplicates(inputTitle)
    inputTitle = inputTitle.replaceAll("[^a-zA-Z0-9&]", "")
    inputTitle = StringUtils.left(inputTitle, 25)
    inputTitle = StringUtils.removeEnd(inputTitle, "S")

    inputTitle
  }

  def formatStemming(input: String): String = {
    val upperCaseString = StringUtils.upperCase(input)
    var formattedString = ""
    for(word <- upperCaseString.split(" ")){
        if(StringUtils.endsWith(word, "ING")){
          val stemmedString = StringUtils.removeEnd(word, "ING")
          formattedString= formattedString+stemmedString+"IN"
        }
      else {
          formattedString= formattedString+word
        }
    }
    formattedString
  }

  def removeConsecutiveDuplicates(input: String ): String = {
    if (input.length() <= 1)
      return input
    if (input.charAt(0) == input.charAt(1))
      return removeConsecutiveDuplicates(
        input.substring(1));
    else
      return input.charAt(0)+ removeConsecutiveDuplicates(input.substring(1));
  }
}
