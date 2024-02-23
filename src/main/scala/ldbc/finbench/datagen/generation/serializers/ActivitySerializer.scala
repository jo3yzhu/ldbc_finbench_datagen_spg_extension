package ldbc.finbench.datagen.generation.serializers

import ldbc.finbench.datagen.entities.edges._
import ldbc.finbench.datagen.entities.nodes._
import ldbc.finbench.datagen.io.raw.RawSink
import ldbc.finbench.datagen.model.raw._
import ldbc.finbench.datagen.syntax._
import ldbc.finbench.datagen.util.{Logging, SparkUI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

/**
 * generate person and company activities
 * */
class ActivitySerializer(sink: RawSink)(implicit spark: SparkSession) extends Serializable with Logging {
  private val options: Map[String, String] = sink.formatOptions ++ Map("header" -> "true", "delimiter" -> "|")
  private val pathPrefix: String = (sink.outputDir / "raw").toString

  private def formattedDouble(d: Double): String = f"$d%.2f"

  def writeCountry(self: RDD[Country]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write Country") {
      val rawCountries = self.map { p: Country =>
        CountryRaw(p.getParentName, p.getCountryName)
      }
      spark.createDataFrame(rawCountries).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "country").toString)
    }
  }

  def writeCity(self: RDD[City]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write City") {
      val rawCities = self.map { p: City =>
        CityRaw(p.getParentName, p.getCityName)
      }
      spark.createDataFrame(rawCities).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "city").toString)
    }
  }

  def writeAccountLevel(self: RDD[AccountLevel]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write AccountLevel") {
      val rawAccountLevels = self.map { p: AccountLevel =>
        AccountLevelRaw(p.getParentName, p.getLevelName)
      }
      spark.createDataFrame(rawAccountLevels).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "accountlevel").toString)
    }
  }

  def writeAccountType(self: RDD[AccountType]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write AccountType") {
      val rawAccountTypes = self.map { p: AccountType =>
        AccountTypeRaw(p.getParentName, p.getTypeName)
      }
      spark.createDataFrame(rawAccountTypes).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "accounttype").toString)
    }
  }

  def writeBusinessType(self: RDD[BusinessType]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write BusinessType") {
      val rawBusinessType = self.map { p: BusinessType =>
        BusinessTypeRaw(p.getParentName, p.getTypeName)
      }
      spark.createDataFrame(rawBusinessType).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "businesstype").toString)
    }
  }

  def writeEmail(self: RDD[Email]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write Email") {
      val rawEmail = self.map { p: Email =>
        BusinessTypeRaw(p.getParentName, p.getTypeName)
      }
      spark.createDataFrame(rawEmail).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "email").toString)
    }
  }

  def writeLoanUsage(self: RDD[LoanUsage]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write LoanUsage") {
      val rawLoanUsage = self.map { p: LoanUsage =>
        LoanUsageRaw(p.getParentName, p.getTypeName)
      }
      spark.createDataFrame(rawLoanUsage).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "loanusage").toString)
    }
  }

  def writeMediumType(self: RDD[MediumType]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write MediumType") {
      val rawMediumType = self.map { p: MediumType =>
        MediumTypeRaw(p.getParentName, p.getTypeName)
      }
      spark.createDataFrame(rawMediumType).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "mediumtype").toString)
    }
  }

  def writeRiskLevel(self: RDD[RiskLevel]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write RiskLevel") {
      val rawRiskLevel = self.map { p: RiskLevel =>
        RiskLevelRaw(p.getParentName, p.getLevelName)
      }
      spark.createDataFrame(rawRiskLevel).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "risklevel").toString)
    }
  }

  def writeUrl(self: RDD[Url]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write Url") {
      val rawUrl = self.map { p: Url =>
        UrlRaw(p.getParentName, p.getUrlName)
      }
      spark.createDataFrame(rawUrl).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "url").toString)
    }
  }

  def writeAccountHasAccountLevel(self: RDD[AccountHasAccountLevel]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write AccountHasAccountLevel") {
      val rawAccountHasAccountLevel = self.map { p: AccountHasAccountLevel =>
        AccountHasAccountLevelRaw(p.getAccount.getAccountId, p.getAccountLevel.getLevelName)
      }
      spark.createDataFrame(rawAccountHasAccountLevel).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "account_has_accountlevel").toString)
    }
  }

  def writeAccountHasAccountType(self: RDD[AccountHasAccountType]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write AccountHasAccountLevel") {
      val rawAccountHasAccountType = self.map { p: AccountHasAccountType =>
        AccountHasAccountTypeRaw(p.getAccount.getAccountId, p.getAccountType.getTypeName)
      }
      spark.createDataFrame(rawAccountHasAccountType).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "account_has_accounttype").toString)
    }
  }

  def writeCompanyHasBusinessType(self: RDD[CompanyHasBusinessType]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write CompanyHasBusinessType") {
      val rawCompanyHasBusinessType = self.map { p: CompanyHasBusinessType =>
        CompanyHasBusinessTypeRaw(p.getCompany.getCompanyId, p.getBusinessType.getTypeName)
      }
      spark.createDataFrame(rawCompanyHasBusinessType).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "company_has_businesstype").toString)
    }
  }

  def writeCompanyBaseCity(self: RDD[CompanyBaseCity]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write CompanyBaseCity") {
      val rawCompanyBaseCity = self.map { p: CompanyBaseCity =>
        CompanyBaseCityRaw(p.getCompany.getCompanyId, p.getCity.getCityName)
      }
      spark.createDataFrame(rawCompanyBaseCity).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "company_base_city").toString)
    }
  }

  def writeCompanyBaseCountry(self: RDD[CompanyBaseCountry]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write CompanyBaseCountry") {
      val rawCompanyBaseCountry = self.map { p: CompanyBaseCountry =>
        CompanyBaseCountryRaw(p.getCompany.getCompanyId, p.getCountry.getCountryName)
      }
      spark.createDataFrame(rawCompanyBaseCountry).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "company_base_country").toString)
    }
  }

  def writeAccountFreqLoginMediumType(self: RDD[AccountFreqLoginMediumType]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write AccountFreqLoginMediumType") {
      val rawAccountFreqLoginMediumType = self.map { p: AccountFreqLoginMediumType =>
        AccountFreqLoginMediumTypeRaw(p.getAccount.getAccountId, p.getMediumType.getTypeName)
      }
      spark.createDataFrame(rawAccountFreqLoginMediumType).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "account_freqlogin_mediumtype").toString)
    }
  }

  def writeAccountHasEmail(self: RDD[AccountHasEmail]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write AccountHasEmail") {
      val rawAccountHasEmail = self.map { p: AccountHasEmail =>
        AccountHasEmailRaw(p.getAccount.getAccountId, p.getEmail.getTypeName)
      }
      spark.createDataFrame(rawAccountHasEmail).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "account_has_email").toString)
    }
  }

  def writeCompanyHasUrl(self: RDD[CompanyHasUrl]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write CompanyHasUrl") {
      val rawCompanyHasUrl = self.map { p: CompanyHasUrl =>
        CompanyHasUrlRaw(p.getCompany.getCompanyId, p.getUrl.getUrlName)
      }
      spark.createDataFrame(rawCompanyHasUrl).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "company_has_url").toString)
    }
  }

  def writeLoanHasLoanUsage(self: RDD[LoanHasLoanUsage]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write LoanHasLoanUsage") {
      val rawLoanHasLoanUsage = self.map { p: LoanHasLoanUsage =>
        LoanHasLoanUsageRaw(p.getLoan.getLoanId, p.getLoanUsage.getTypeName)
      }
      spark.createDataFrame(rawLoanHasLoanUsage).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "loan_has_loanusage").toString)
    }
  }

  def writeMediumHasMediumType(self: RDD[MediumHasMediumType]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write MediumHasMediumType") {
      val rawMediumHasMediumType = self.map { p: MediumHasMediumType =>
        MediumHasMediumTypeRaw(p.getMedium.getMediumId, p.getMediumType.getTypeName)
      }
      spark.createDataFrame(rawMediumHasMediumType).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "medium_has_mediumtype").toString)
    }
  }

  def writeMediumHasRiskLevel(self: RDD[MediumHasRiskLevel]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write MediumHasRiskLevel") {
      val rawMediumHasRiskLevel = self.map { p: MediumHasRiskLevel =>
        MediumHasRiskLevelRaw(p.getMedium.getMediumId, p.getRiskLevel.getLevelName)
      }
      spark.createDataFrame(rawMediumHasRiskLevel).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "medium_has_risklevel").toString)
    }
  }

  def writePersonLiveInCity(self: RDD[PersonLiveInCity]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write PersonLiveInCity") {
      val rawPersonLiveInCity = self.map { p: PersonLiveInCity =>
        PersonLiveInCityRaw(p.getPerson.getPersonId, p.getCity.getCityName)
      }
      spark.createDataFrame(rawPersonLiveInCity).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "person_livein_city").toString)
    }
  }

  def writePersonLiveInCountry(self: RDD[PersonLiveInCountry]): Unit = {
    SparkUI.jobAsync("Write Semantic Property Graph", "Write PersonLiveInCity") {
      val rawPersonLiveInCountry = self.map { p: PersonLiveInCountry =>
        PersonLiveInCountryRaw(p.getPerson.getPersonId, p.getCountry.getCountryName)
      }
      spark.createDataFrame(rawPersonLiveInCountry).write.format(sink.format.toString).options(options)
        .save((pathPrefix / "person_livein_country").toString)
    }
  }

  def writePersonWithActivities(self: RDD[Person]): Unit = {
    SparkUI.jobAsync("Write Person", "Write Person") {
      val rawPersons = self.map { p: Person =>
        PersonRaw(p.getPersonId, p.getCreationDate, p.getPersonName, p.isBlocked,
                  p.getGender, p.getBirthday, p.getCountryName, p.getCityName)
      }
      spark.createDataFrame(rawPersons).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "person").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person own account") {
      val rawPersonOwnAccount = self.flatMap { p =>
        p.getPersonOwnAccounts.asScala.map { poa =>
          PersonOwnAccountRaw(p.getPersonId, poa.getAccount.getAccountId,
                              poa.getCreationDate, poa.getDeletionDate, poa.isExplicitlyDeleted)
        }
      }
      spark.createDataFrame(rawPersonOwnAccount).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "personOwnAccount").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person guarantee") {
      val rawPersonGuarantee = self.flatMap { p =>
        p.getGuaranteeSrc.asScala.map {
          pgp: PersonGuaranteePerson =>
            PersonGuaranteePersonRaw(pgp.getFromPerson.getPersonId,
                                     pgp.getToPerson.getPersonId,
                                     pgp.getCreationDate, pgp.getRelationship)
        }
      }
      spark.createDataFrame(rawPersonGuarantee).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "personGuarantee").toString)
    }

    SparkUI.jobAsync("Write Person", "Write Person apply loan") {
      val rawPersonLoan = self.flatMap { p =>
        p.getPersonApplyLoans.asScala.map {
          pal: PersonApplyLoan =>
            PersonApplyLoanRaw(pal.getPerson.getPersonId,
                               pal.getLoan.getLoanId, formattedDouble(pal.getLoan.getLoanAmount),
                               pal.getCreationDate, pal.getOrganization)
        }
      }
      spark.createDataFrame(rawPersonLoan).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "personApplyLoan").toString)
    }
  }

  def writeCompanyWithActivities(self: RDD[Company]): Unit = {
    SparkUI.jobAsync("Write Company", "Write Company") {
      val rawCompanies = self.map { c: Company =>
        CompanyRaw(c.getCompanyId, c.getCreationDate, c.getCompanyName, c.isBlocked,
                   c.getCountryName, c.getCityName, c.getBusiness, c.getDescription, c.getUrl)
      }
      spark.createDataFrame(rawCompanies).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "company").toString)

      val rawCompanyOwnAccount = self.flatMap { c =>
        c.getCompanyOwnAccounts.asScala.map { coa =>
          CompanyOwnAccountRaw(c.getCompanyId, coa.getAccount.getAccountId,
                               coa.getCreationDate, coa.getDeletionDate, coa.isExplicitlyDeleted)
        }
      }
      spark.createDataFrame(rawCompanyOwnAccount).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "companyOwnAccount").toString)
    }

    SparkUI.jobAsync("Write Company", "Write Company guarantee") {
      val rawCompanyGuarantee = self.flatMap { c =>
        c.getGuaranteeSrc.asScala.map {
          cgc: CompanyGuaranteeCompany =>
            CompanyGuaranteeCompanyRaw(cgc.getFromCompany.getCompanyId,
                                       cgc.getToCompany.getCompanyId,
                                       cgc.getCreationDate, cgc.getRelationship)
        }
      }
      spark.createDataFrame(rawCompanyGuarantee).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "companyGuarantee").toString)
    }

    SparkUI.jobAsync("Write Company", "Write Company apply loan") {
      val rawCompanyLoan = self.flatMap { c =>
        c.getCompanyApplyLoans.asScala.map {
          cal: CompanyApplyLoan =>
            CompanyApplyLoanRaw(cal.getCompany.getCompanyId,
                                cal.getLoan.getLoanId,
                                formattedDouble(cal.getLoan.getLoanAmount), cal.getCreationDate, cal.getOrganization)
        }
      }
      spark.createDataFrame(rawCompanyLoan).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "companyApplyLoan").toString)
    }
  }

  def writeMediumWithActivities(media: RDD[Medium], signIns: RDD[SignIn]): Unit = {
    SparkUI.jobAsync("Write media", "Write Medium") {
      val rawMedium = media
        .map { m: Medium =>
          MediumRaw(m.getMediumId, m.getCreationDate, m.getMediumName, m.isBlocked,
                    m.getLastLogin, m.getRiskLevel)
        }
      spark.createDataFrame(rawMedium).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "medium").toString)

      val rawSignIn = signIns.map { si: SignIn =>
        SignInRaw(si.getMedium.getMediumId, si.getAccount.getAccountId,
                  si.getMultiplicityId, si.getCreationDate, si.getDeletionDate, si.isExplicitlyDeleted, si.getLocation)
      }
      spark.createDataFrame(rawSignIn).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "signIn").toString)
    }
  }

  def writeAccountWithActivities(self: RDD[Account], transfers: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write Account", "Write Account") {
      val rawAccount = self.map { a: Account =>
        AccountRaw(a.getAccountId, a.getCreationDate, a.getDeletionDate, a.isBlocked,
                   a.getType, a.getNickname, a.getPhonenum, a.getEmail, a.getFreqLoginType,
                   a.getLastLoginTime, a.getAccountLevel, a.getMaxInDegree, a.getMaxOutDegree, a.isExplicitlyDeleted,
                   a.getOwnerType.toString)
      }
      spark.createDataFrame(rawAccount).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "account").toString)

      val rawTransfer = transfers.map { t =>
        TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId,
                    t.getMultiplicityId, t.getCreationDate, t.getDeletionDate, formattedDouble(t.getAmount),
                    t.isExplicitlyDeleted,
                    t.getOrdernum, t.getComment, t.getPayType, t.getGoodsType)
      }
      spark.createDataFrame(rawTransfer).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "transfer").toString)
    }
  }

  def writeWithdraw(self: RDD[Withdraw]): Unit = {
    SparkUI.jobAsync("Write withdraw", "Write Withdraw") {
      val rawWithdraw = self.map {
        w =>
          WithdrawRaw(w.getFromAccount.getAccountId, w.getToAccount.getAccountId,
                      w.getFromAccount.getType, w.getToAccount.getType, w.getMultiplicityId, w.getCreationDate,
                      w.getDeletionDate, formattedDouble(w.getAmount), w.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawWithdraw).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "withdraw").toString)
    }
  }

  def writeInvest(self: RDD[Either[PersonInvestCompany, CompanyInvestCompany]]): Unit = {
    SparkUI.jobAsync("Write invest", "Write Person Invest") {
      val personInvest = self.filter(_.isLeft).map(_.left.get)
      spark.createDataFrame(personInvest.map { pic =>
        PersonInvestCompanyRaw(pic.getPerson.getPersonId, pic.getCompany.getCompanyId,
                               pic.getCreationDate, pic.getRatio)
      }).write.format(sink.format.toString).options(options).save((pathPrefix / "personInvest").toString)
    }

    SparkUI.jobAsync("Write invest", "Write Company Invest") {
      val companyInvest = self.filter(_.isRight).map(_.right.get)
      spark.createDataFrame(companyInvest.map { cic =>
        CompanyInvestCompanyRaw(cic.getFromCompany.getCompanyId,
                                cic.getToCompany.getCompanyId, cic.getCreationDate, cic.getRatio)
      }).write.format(sink.format.toString).options(options).save((pathPrefix / "companyInvest").toString)
    }
  }

  def writeLoanActivities(self: RDD[Loan], deposits: RDD[Deposit], repays: RDD[Repay],
                          loantransfers: RDD[Transfer]): Unit = {
    SparkUI.jobAsync("Write loan", "Write Loan") {
      val rawLoan = self.map { l: Loan =>
        LoanRaw(l.getLoanId, l.getCreationDate, formattedDouble(l.getLoanAmount), formattedDouble(l.getBalance),
                l.getUsage, f"${l.getInterestRate}%.3f")
      }
      spark.createDataFrame(rawLoan).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "loan").toString)

      val rawDeposit = deposits.map { d: Deposit =>
        DepositRaw(d.getLoan.getLoanId, d.getAccount.getAccountId,
                   d.getCreationDate, d.getDeletionDate, formattedDouble(d.getAmount), d.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawDeposit).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "deposit").toString)

      val rawRepay = repays.map { r: Repay =>
        RepayRaw(r.getAccount.getAccountId, r.getLoan.getLoanId,
                 r.getCreationDate, r.getDeletionDate, formattedDouble(r.getAmount), r.isExplicitlyDeleted)
      }
      spark.createDataFrame(rawRepay).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "repay").toString)

      val rawLoanTransfer = loantransfers.map { t: Transfer =>
        TransferRaw(t.getFromAccount.getAccountId, t.getToAccount.getAccountId, t.getMultiplicityId, t.getCreationDate,
                    t.getDeletionDate, formattedDouble(t.getAmount), t.isExplicitlyDeleted, t.getOrdernum, t.getComment,
                    t.getPayType, t.getGoodsType)
      }
      spark.createDataFrame(rawLoanTransfer).write.format(sink.format.toString).options(options)
           .save((pathPrefix / "loantransfer").toString)
    }
  }
}
