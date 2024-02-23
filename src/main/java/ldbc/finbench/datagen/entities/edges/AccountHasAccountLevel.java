package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.AccountLevel;

public class AccountHasAccountLevel implements Serializable {
    private Account account;
    private AccountLevel accountLevel;

    public AccountHasAccountLevel(Account account, AccountLevel accountLevel) {
        this.account = account;
        this.accountLevel = accountLevel;
    }

    public Account getAccount() {
        return account;
    }

    public AccountLevel getAccountLevel() {
        return accountLevel;
    }
}
