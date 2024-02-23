package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.City;
import ldbc.finbench.datagen.entities.nodes.Person;

public class PersonLiveInCity implements Serializable {
    private Person person;
    private City city;

    public PersonLiveInCity(Person person, City city) {
        this.person = person;
        this.city = city;
    }

    public Person getPerson() {
        return person;
    }

    public City getCity() {
        return city;
    }
}
