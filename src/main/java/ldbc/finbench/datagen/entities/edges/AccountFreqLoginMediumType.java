package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.MediumType;

public class AccountFreqLoginMediumType implements Serializable {
    private Account account;
    private MediumType mediumType;

    public AccountFreqLoginMediumType(Account account, MediumType mediumType) {
        this.account = account;
        this.mediumType = mediumType;
    }

    public Account getAccount() {
        return account;
    }

    public MediumType getMediumType() {
        return mediumType;
    }
}
