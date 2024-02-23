package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.PersonLiveInCountry
import ldbc.finbench.datagen.entities.nodes.Person
import ldbc.finbench.datagen.entities.nodes.Country
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkPersonLiveInCountryGenerator {
  def apply(persons: RDD[Person], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[PersonLiveInCountry] = {

    val transformer: Person => PersonLiveInCountry = person => {
      new PersonLiveInCountry(person, new Country(person.getCountryName))
    }

    persons.map(transformer)
  }
}