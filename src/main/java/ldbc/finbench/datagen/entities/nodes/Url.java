package ldbc.finbench.datagen.entities.nodes;

public class Url {
    private String parentName = "None";
    private String urlName;

    public Url(String urlName) {
        this.urlName = urlName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getParentName() {
        return parentName;
    }

    public void setUrlName(String urlName) {
        this.urlName = urlName;
    }

    public String getUrlName() {
        return urlName;
    }
}
