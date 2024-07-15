import ch.hsr.geohash.GeoHash
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}

import java.net.URLEncoder
import scala.util.parsing.json.JSON

object Main {
  def main(args: Array[String]): Unit = {

    // This function uses city and country name and gives us Double(latitude, longitude) in exchange. Example: geocode("Russia","Moscow") => 55.7558, 37.6173
    val geocode = udf((country: String, city: String) => {

      val API_key = "" // Your API key to connect to OpenCageData
      val encodedCity = URLEncoder.encode(city, "UTF-8") // Encode 'city' to avoid space and symbols to crush url
      val url = s"https://api.opencagedata.com/geocode/v1/json?q=$encodedCity,$country&key=$API_key"

      // GET request
      val client = HttpClients.createDefault()
      val request = new HttpGet(url)
      val response = client.execute(request)

      // Parse JSON response from OpenCage API
      val json: String = EntityUtils.toString(response.getEntity) // Convert HTTP-entity to string
      val result = JSON.parseFull(json).get.asInstanceOf[Map[String, Any]]
      val geometry = result("results").asInstanceOf[List[Map[String, Any]]].head("geometry").asInstanceOf[Map[String, Any]] // Extracts "results" from the result map, then casts it to the List[Map]
      val lat = geometry("lat").asInstanceOf[Double]
      val lng = geometry("lng").asInstanceOf[Double]

      (lat, lng)
    })

    // Creating SparkSession and perform loading files
    val spark = SparkSession.builder().appName("geocoder").master("local[*]").getOrCreate()
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/xl3f/Desktop/untitled7/hotels")
    val hotelsWithLocation = df.filter(col("Latitude").isNull || col("Longitude").isNull) //filter for Null's
    val hotelsWithGeocodedLocation = hotelsWithLocation.withColumn("Location", geocode(col("Country"), col("City")))
      .withColumn("Latitude", col("Location._1"))
      .withColumn("Longitude", col("Location._2"))
      .drop("Location") //replace nulls
    val updatedHotels = hotelsWithGeocodedLocation.unionAll(df.filter(col("Latitude").isNotNull && col("Longitude").isNotNull))

    val geohashUDF = udf((lat: Double, lon: Double, len: Int) => {
      GeoHash.withCharacterPrecision(lat, lon, len).toBase32
    })
    // Add geohash column to hotels data, read weather data and add geohash column to the weather data
    val dfWithGeohash = updatedHotels.withColumn("Geohash", geohashUDF(col("Latitude"), col("Longitude"), lit(4)))
    val weatherDf = spark.read.format("parquet").option("header", "true").load("/home/xl3f/Desktop/untitled7/weather")
    val weatherDfWithGeohash = weatherDf.withColumn("Geohash", geohashUDF(col("lat"), col("lng"), lit(4)))
    // Left join data from hotels and weather geohashed
    val joinedDf = dfWithGeohash.join(weatherDfWithGeohash, Seq("Geohash"), "left")
    joinedDf.show()
    // Write to disk using the 'weather' partition schema, formatted as Parquet
    //joinedDf.write.partitionBy("year", "month", "day").mode("overwrite").format("parquet").save("/home/xl3f/Desktop/untitled7/output/weather_part")
    spark.stop()
  }
}
