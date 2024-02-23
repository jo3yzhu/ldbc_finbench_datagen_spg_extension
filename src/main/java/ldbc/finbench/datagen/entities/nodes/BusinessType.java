package ldbc.finbench.datagen.entities.nodes;

public class BusinessType {
    private String parentName = "None";
    private String typeName;

    public BusinessType(String typeName) {
        this.typeName = typeName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getParentName() {
        return parentName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }
}
