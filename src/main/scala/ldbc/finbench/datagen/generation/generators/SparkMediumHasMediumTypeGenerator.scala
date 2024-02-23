package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.MediumHasMediumType
import ldbc.finbench.datagen.entities.nodes.Medium
import ldbc.finbench.datagen.entities.nodes.MediumType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkMediumHasMediumTypeGenerator {
  def apply(accounts: RDD[Medium], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[MediumHasMediumType] = {

    val transformer: Medium => MediumHasMediumType = medium => {
      new MediumHasMediumType(medium, new MediumType(medium.getMediumName))
    }

    accounts.map(transformer)
  }
}