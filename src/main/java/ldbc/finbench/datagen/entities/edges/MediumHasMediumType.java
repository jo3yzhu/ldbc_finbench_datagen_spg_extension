package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.entities.nodes.MediumType;

public class MediumHasMediumType implements Serializable {
    private Medium medium;
    private MediumType mediumType;

    public MediumHasMediumType(Medium medium, MediumType mediumType) {
        this.medium = medium;
        this.mediumType = mediumType;
    }

    public Medium getMedium() {
        return medium;
    }

    public MediumType getMediumType() {
        return mediumType;
    }
}
