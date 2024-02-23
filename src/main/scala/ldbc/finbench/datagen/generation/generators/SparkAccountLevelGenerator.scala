package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.AccountLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkAccountLevelGenerator {
  def apply(accountLevels: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[AccountLevel] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(accountLevels.toSeq, numSlices = partitions)
      .map { case (name) => new AccountLevel(name)}
  }
}