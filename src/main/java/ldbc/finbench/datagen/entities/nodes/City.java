package ldbc.finbench.datagen.entities.nodes;

public class City {
    private String parentName = "None";
    private String cityName;

    public City(String cityName) {
        this.cityName = cityName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getParentName() {
        return parentName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getCityName() {
        return cityName;
    }
}
