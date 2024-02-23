package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.MediumHasRiskLevel
import ldbc.finbench.datagen.entities.nodes.Medium
import ldbc.finbench.datagen.entities.nodes.RiskLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkMediumHasRiskLevelGenerator {
  def apply(accounts: RDD[Medium], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[MediumHasRiskLevel] = {

    val transformer: Medium => MediumHasRiskLevel = medium => {
      new MediumHasRiskLevel(medium, new RiskLevel(medium.getRiskLevel))
    }

    accounts.map(transformer)
  }
}