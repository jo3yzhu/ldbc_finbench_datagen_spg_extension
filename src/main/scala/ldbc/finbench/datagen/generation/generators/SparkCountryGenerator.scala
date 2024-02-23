package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.Country
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkCountryGenerator {
  def apply(countryDict: Map[String, Integer], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[Country] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(countryDict.toSeq, numSlices = partitions)
      .map { case (name, id) => new Country(name)}
  }
}