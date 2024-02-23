package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Company;
import ldbc.finbench.datagen.entities.nodes.Url;

public class CompanyHasUrl implements Serializable {
    private Company company;
    private Url url;

    public CompanyHasUrl(Company company, Url url) {
        this.company = company;
        this.url = url;
    }

    public Company getCompany() {
        return company;
    }

    public Url getUrl() {
        return url;
    }
}
