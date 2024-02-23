package ldbc.finbench.datagen.entities.nodes;

public class RiskLevel {
    private String parentName = "None";
    private String levelName;

    public RiskLevel(String levelName) {
        this.levelName = levelName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getParentName() {
        return parentName;
    }

    public void setLevelName(String levelName) {
        this.levelName = levelName;
    }

    public String getLevelName() {
        return levelName;
    }
}
