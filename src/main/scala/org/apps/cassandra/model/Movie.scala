package org.apps.cassandra.model
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType
case class Movie(
    id: Integer,
    title: String,
    duration: Integer,
    rating: Double,
    year: Integer
)
object Movie {
  val movieSchema: StructType = Encoders.product[Movie].schema
}
