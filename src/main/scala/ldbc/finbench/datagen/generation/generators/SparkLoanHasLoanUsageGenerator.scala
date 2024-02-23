package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.LoanHasLoanUsage
import ldbc.finbench.datagen.entities.nodes.Loan
import ldbc.finbench.datagen.entities.nodes.LoanUsage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkLoanHasLoanUsageGenerator {
  def apply(accounts: RDD[Loan], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[LoanHasLoanUsage] = {

    val transformer: Loan => LoanHasLoanUsage = loan => {
      new LoanHasLoanUsage(loan, new LoanUsage(loan.getUsage))
    }

    accounts.map(transformer)
  }
}