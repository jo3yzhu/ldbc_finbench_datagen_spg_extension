package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.AccountType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkAccountTypeGenerator {
  def apply(accoutTypes: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[AccountType] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(accoutTypes.toSeq, numSlices = partitions)
      .map { case (name) => new AccountType(name)}
  }
}