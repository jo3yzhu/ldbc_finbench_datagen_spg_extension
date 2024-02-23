package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.BusinessType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkBusinessTypeGenerator {
  def apply(businessTypes: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[BusinessType] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(businessTypes.toSeq, numSlices = partitions)
      .map { case (name) => new BusinessType(name)}
  }
}