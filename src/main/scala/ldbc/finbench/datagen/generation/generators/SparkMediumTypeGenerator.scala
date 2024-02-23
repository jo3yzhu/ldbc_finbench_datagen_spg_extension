package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.MediumType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkMediumTypeGenerator {
  def apply(mediumTypes: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[MediumType] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(mediumTypes.toSeq, numSlices = partitions)
      .map { case (name) => new MediumType(name)}
  }
}