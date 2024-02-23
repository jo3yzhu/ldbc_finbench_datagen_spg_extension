package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.AccountHasAccountLevel
import ldbc.finbench.datagen.entities.nodes.AccountLevel
import ldbc.finbench.datagen.entities.nodes.Account
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkAccountHasAccountLevelGenerator {
  def apply(accounts: RDD[Account], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[AccountHasAccountLevel] = {

    val transformer: Account => AccountHasAccountLevel = account => {
      new AccountHasAccountLevel(account, new AccountLevel(account.getAccountLevel))
    }

    accounts.map(transformer)
  }
}