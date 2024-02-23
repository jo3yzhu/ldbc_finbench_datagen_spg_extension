package ldbc.finbench.datagen.entities.edges;

import java.io.Serializable;
import ldbc.finbench.datagen.entities.nodes.Loan;
import ldbc.finbench.datagen.entities.nodes.LoanUsage;


public class LoanHasLoanUsage implements Serializable {
    private Loan loan;
    private LoanUsage loanUsage;

    public LoanHasLoanUsage(Loan loan, LoanUsage loanUsage) {
        this.loan = loan;
        this.loanUsage = loanUsage;
    }

    public Loan getLoan() {
        return loan;
    }

    public LoanUsage getLoanUsage() {
        return loanUsage;
    }
}
