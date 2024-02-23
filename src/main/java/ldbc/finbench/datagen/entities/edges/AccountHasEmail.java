package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Account;
import ldbc.finbench.datagen.entities.nodes.Email;

public class AccountHasEmail implements Serializable {
    private Account account;
    private Email email;

    public AccountHasEmail(Account account, Email email) {
        this.account = account;
        this.email = email;
    }

    public Account getAccount() {
        return account;
    }

    public Email getEmail() {
        return email;
    }
}
