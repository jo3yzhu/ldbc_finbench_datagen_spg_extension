package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.CompanyHasUrl
import ldbc.finbench.datagen.entities.nodes.Company
import ldbc.finbench.datagen.entities.nodes.Url
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkCompanyHasUrlGenerator {
  def apply(companies: RDD[Company], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[CompanyHasUrl] = {

    val transformer: Company => CompanyHasUrl = company => {
      new CompanyHasUrl(company, new Url(company.getUrl))
    }

    companies.map(transformer)
  }
}