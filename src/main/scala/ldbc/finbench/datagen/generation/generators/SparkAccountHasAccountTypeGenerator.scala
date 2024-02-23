package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.AccountHasAccountType
import ldbc.finbench.datagen.entities.nodes.AccountType
import ldbc.finbench.datagen.entities.nodes.Account
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkAccountHasAccountTypeGenerator {
  def apply(accounts: RDD[Account], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[AccountHasAccountType] = {

    val transformer: Account => AccountHasAccountType = account => {
      new AccountHasAccountType(account, new AccountType(account.getType))
    }

    accounts.map(transformer)
  }
}