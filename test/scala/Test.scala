import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import java.util

class Test extends AnyFunSuite {

  def mockData(data: util.List[Row]): DataFrame = {

    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

    val cSchema = StructType(Array(
      StructField("peer_id", StringType),
      StructField("id_1", StringType),
      StructField("id_2", StringType),
      StructField("year", IntegerType)))

    spark.createDataFrame(data, cSchema)
  }

  test("the example with k = 3") {
    val result = Main.process(mockData(util.Arrays.asList(
      Row("ABC17969(AB)", "1", "ABC17969", 2022),
      Row("ABC17969(AB)", "2", "CDC52533", 2022),
      Row("ABC17969(AB)", "3", "DEC59161", 2023),
      Row("ABC17969(AB)", "4", "F43874*", 2022),
      Row("ABC17969(AB)", "5", "MY06154", 2021),
      Row("ABC17969(AB)", "6", "MY4387", 2022),
      Row("AE686(AE)", "7", "AE686", 2023),
      Row("AE686(AE)", "8", "BH2740", 2021),
      Row("AE686(AE)", "9", "EG999", 2021),
      Row("AE686(AE)", "10", "AE0908", 2021),
      Row("AE686(AE)", "11", "QA402", 2022),
      Row("AE686(AE)", "12", "ОМ691", 2022))), 3)

    result.show(false)
    //assert(result.collect() sameElements Array(Row("ABC17969(AB)", 2022), Row("AE686(AE)", 2023), Row("AE686(AE)", 2022)))
  }

  test("the example 2 with k = 5 and k = 7") {
    val dataFrame = mockData(util.Arrays.asList(
      Row("AE686(AE)", "7", "AE686", 2022),
      Row("AE686(AE)", "8", "BH2740", 2021),
      Row("AE686(AE)", "9", "EG999", 2021),
      Row("AE686(AE)", "10", "AE0908", 2023),
      Row("AE686(AE)", "11", "QA402", 2022),
      Row("AE686(AE)", "12", "ОA691", 2022),
      Row("AE686(AE)", "12", "ОB691", 2022),
      Row("AE686(AE)", "12", "ОC691", 2019),
      Row("AE686(AE)", "12", "ОD691", 2017)))
      var result = Main.process(dataFrame, 5)
      result.show(false)
      result = Main.process(dataFrame, 7)
      result.show(false)
      assert(result.collect() sameElements Array(Row("AE686(AE)", 2022), Row("AE686(AE)", 2021), Row("AE686(AE)", 2019)))
  }

}
