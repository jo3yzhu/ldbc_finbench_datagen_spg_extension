package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.Url
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkUrlGenerator {
  def apply(urls: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[Url] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(urls.toSeq, numSlices = partitions)
      .map { case (name) => new Url(name)}
  }
}