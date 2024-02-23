package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.BusinessType;
import ldbc.finbench.datagen.entities.nodes.Company;

public class CompanyHasBusinessType implements Serializable {
    private Company company;
    private BusinessType businessType;

    public CompanyHasBusinessType(Company company, BusinessType businessType) {
        this.company = company;
        this.businessType = businessType;
    }

    public Company getCompany() {
        return company;
    }

    public BusinessType getBusinessType() {
        return businessType;
    }
}
