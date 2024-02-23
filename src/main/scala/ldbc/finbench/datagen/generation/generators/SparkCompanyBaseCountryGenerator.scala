package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.CompanyBaseCountry
import ldbc.finbench.datagen.entities.nodes.Company
import ldbc.finbench.datagen.entities.nodes.Country
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkCompanyBaseCountryGenerator {
  def apply(companies: RDD[Company], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[CompanyBaseCountry] = {

    val transformer: Company => CompanyBaseCountry = company => {
      new CompanyBaseCountry(company, new Country(company.getCompanyName))
    }

    companies.map(transformer)
  }
}