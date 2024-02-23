package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.nodes.BusinessType
import ldbc.finbench.datagen.entities.nodes.Company
import ldbc.finbench.datagen.entities.edges.CompanyHasBusinessType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkCompanyHasBusinessTypeGenerator {
  def apply(companies: RDD[Company], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[CompanyHasBusinessType] = {

    val transformer: Company => CompanyHasBusinessType = company => {
      new CompanyHasBusinessType(company, new BusinessType(company.getBusiness))
    }

    companies.map(transformer)
  }
}