package ldbc.finbench.datagen.entities.nodes;

public class Country {
    private String parentName = "None";
    private String countryName;

    public Country(String countryName) {
        this.countryName = countryName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getParentName() {
        return parentName;
    }

    public void setCountryName(String countryName) {
        this.countryName = countryName;
    }

    public String getCountryName() {
        return countryName;
    }
}
