package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.CompanyBaseCity
import ldbc.finbench.datagen.entities.nodes.Company
import ldbc.finbench.datagen.entities.nodes.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkCompanyBaseCityGenerator {
  def apply(companies: RDD[Company], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[CompanyBaseCity] = {

    val transformer: Company => CompanyBaseCity = company => {
      new CompanyBaseCity(company, new City(company.getCityName))
    }

    companies.map(transformer)
  }
}