package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Country;

public class CompanyBaseCountry implements Serializable {
    private Company company;
    private Country country;

    public CompanyBaseCountry(Company company, Country country) {
        this.company = company;
        this.country = country;
    }

    public Company getCompany() {
        return company;
    }

    public Country getCountry() {
        return country;
    }
}
