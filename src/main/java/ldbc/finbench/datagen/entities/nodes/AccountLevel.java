package ldbc.finbench.datagen.entities.nodes;

public class AccountLevel {
    private String parentName = "None";
    private String levelName;

    public AccountLevel(String levelName) {
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
