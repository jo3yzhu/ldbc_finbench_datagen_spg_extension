package ldbc.finbench.datagen.entities.nodes;

public class AccountType {
    private String parentName = "None";
    private String typeName;

    public AccountType(String typeName) {
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
