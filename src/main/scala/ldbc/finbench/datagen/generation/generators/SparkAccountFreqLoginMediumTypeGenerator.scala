package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.AccountFreqLoginMediumType
import ldbc.finbench.datagen.entities.nodes.Account
import ldbc.finbench.datagen.entities.nodes.MediumType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkAccountFreqLoginMediumTypeGenerator {
  def apply(accounts: RDD[Account], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[AccountFreqLoginMediumType] = {

    val transformer: Account => AccountFreqLoginMediumType = account => {
      new AccountFreqLoginMediumType(account, new MediumType(account.getFreqLoginType))
    }

    accounts.map(transformer)
  }
}