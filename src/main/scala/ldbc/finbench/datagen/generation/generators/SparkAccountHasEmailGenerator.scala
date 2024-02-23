package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.AccountHasEmail
import ldbc.finbench.datagen.entities.nodes.Account
import ldbc.finbench.datagen.entities.nodes.Email
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkAccountHasEmailGenerator {
  def apply(accounts: RDD[Account], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[AccountHasEmail] = {

    val transformer: Account => AccountHasEmail = account => {
      new AccountHasEmail(account, new Email(account.getEmail))
    }

    accounts.map(transformer)
  }
}