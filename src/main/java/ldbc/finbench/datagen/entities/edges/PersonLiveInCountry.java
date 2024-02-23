package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Country;
import ldbc.finbench.datagen.entities.nodes.Person;

public class PersonLiveInCountry implements Serializable {
    private Person person;
    private Country country;

    public PersonLiveInCountry(Person person, Country country) {
        this.person = person;
        this.country = country;
    }

    public Person getPerson() {
        return person;
    }

    public Country getCountry() {
        return country;
    }
}
