package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.Email
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkEmailGenerator {
  def apply(emails: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[Email] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(emails.toSeq, numSlices = partitions)
      .map { case (name) => new Email(name)}
  }
}