package ldbc.finbench.datagen.generation.generators

import ldbc.finbench.datagen.entities.edges.PersonLiveInCity
import ldbc.finbench.datagen.entities.nodes.Person
import ldbc.finbench.datagen.entities.nodes.City
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map;

object SparkPersonLiveInCityGenerator {
  def apply(persons: RDD[Person], numPartitions: Option[Int] = None)(
    implicit spark: SparkSession): RDD[PersonLiveInCity] = {

    val transformer: Person => PersonLiveInCity = person => {
      new PersonLiveInCity(person, new City(person.getCityName))
    }

    persons.map(transformer)
  }
}