package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Medium;
import ldbc.finbench.datagen.entities.nodes.RiskLevel;


public class MediumHasRiskLevel implements Serializable {
    private Medium medium;
    private RiskLevel riskLevel;

    public MediumHasRiskLevel(Medium medium, RiskLevel riskLevel) {
        this.medium = medium;
        this.riskLevel = riskLevel;
    }

    public Medium getMedium() {
        return medium;
    }

    public RiskLevel getRiskLevel() {
        return riskLevel;
    }
}
