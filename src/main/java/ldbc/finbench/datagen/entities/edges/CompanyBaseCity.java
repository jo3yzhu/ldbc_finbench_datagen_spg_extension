package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.City;
import ldbc.finbench.datagen.entities.nodes.Company;

public class CompanyBaseCity implements Serializable {
    private Company company;
    private City city;

    public CompanyBaseCity(Company company, City city) {
        this.company = company;
        this.city = city;
    }

    public Company getCompany() {
        return company;
    }

    public City getCity() {
        return city;
    }
}
