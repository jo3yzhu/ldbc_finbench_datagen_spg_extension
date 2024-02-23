package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.RiskLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkRiskLevelGenerator {
  def apply(riskLevels: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[RiskLevel] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(riskLevels.toSeq, numSlices = partitions)
      .map { case (name) => new RiskLevel(name)}
  }
}