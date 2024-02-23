package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.LoanUsage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkLoanUsageGenerator {
  def apply(loanUsages: List[String], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[LoanUsage] = {

    val partitions = numPartitions.getOrElse(spark.sparkContext.defaultParallelism)

    spark.sparkContext.parallelize(loanUsages.toSeq, numSlices = partitions)
      .map { case (name) => new LoanUsage(name)}
  }
}