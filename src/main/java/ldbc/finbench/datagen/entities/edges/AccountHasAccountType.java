package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.AccountType;

public class AccountHasAccountType implements Serializable {
    private Account account;
    private AccountType accountType;

    public AccountHasAccountType(Account account, AccountType accountType) {
        this.account = account;
        this.accountType = accountType;
    }

    public Account getAccount() {
        return account;
    }

    public AccountType getAccountType() {
        return accountType;
    }
}
