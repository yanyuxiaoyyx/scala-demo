import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.{DataFrame, functions}

object Main {
  def process(dataFrame: DataFrame, k: Int): DataFrame = {
    // step 1: find the year in which peer_id contains id_2
    val years = dataFrame.filter(r => r.getString(0).contains(r.getString(2))).groupBy("peer_id").max("year").toDF("peer_id", "wanted_year")

    // prepare window for step 2, used to get total appearances after current year
    val laterYearsWindow = partitionBy("peer_id").orderBy(desc("year")).rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)

    dataFrame.join(years, "peer_id").filter(r => r.getInt(3) <= r.getInt(4)).groupBy("peer_id", "year").count()
      .toDF("peer_id", "year", "appearance")
      //total after current year
      .withColumn("total_after_years", functions.sum("appearance").over(laterYearsWindow))
      //total for peer
      .withColumn("total", functions.sum("appearance").over(partitionBy("peer_id")))
      //only peers whose total is no less than k, and years with not enough later appearance are wanted
      .filter(r => r.getLong(4) >= k && (r.get(3) == null || r.getLong(3) < k))
      .orderBy(asc("peer_id"), desc("year"))
      .select("peer_id", "year")

  }
}