package ldbc.finbench.datagen.model

import org.joda.time.DateTime

/**
  * define LDBC Finbench Data Schema
  */
object raw {

  // =========================================
  // extension for semantic property graph
  // =========================================

  // define country entity
  case class CountryRaw(parentName: String, countryName: String)

  // define city entity
  case class CityRaw(parentName: String, cityName: String)

  // define account level entity
  case class AccountLevelRaw(parentName: String, levelName: String)

  // define account type entity
  case class AccountTypeRaw(parentName: String, typeName: String)

  // define business type entity
  case class BusinessTypeRaw(parentName: String, typeName: String)

  // define email entity
  case class EmailRaw(parentName: String, typeName: String)

  // define loan usage entity
  case class LoanUsageRaw(parentName: String, typeName: String)

  // define medium type entity
  case class MediumTypeRaw(parentName: String, typeName: String)

  // define risk level entity
  case class RiskLevelRaw(parentName: String, levelName: String)

  // define url entity
  case class UrlRaw(parentName: String, urlName: String)

  // define AccountHasAccountLevel relationship
  case class AccountHasAccountLevelRaw(accountId: Long, accountLevelName: String)

  // define AccountHasAccountType relationship
  case class AccountHasAccountTypeRaw(accountId: Long, accountTypeName: String)

  // define CompanyHasBusinessType relationship
  case class CompanyHasBusinessTypeRaw(companyId: Long, businessTypeName: String)

  // define CompanyBaseCity relationship
  case class CompanyBaseCityRaw(companyId: Long, cityName: String)

  // define PersonLiveInCity relationship
  case class PersonLiveInCityRaw(personId: Long, cityName: String)

  // define CompanyBaseCountry relationship
  case class CompanyBaseCountryRaw(companyId: Long, countryName: String)

  // define PersonLiveInCountry relationship
  case class PersonLiveInCountryRaw(personId: Long, countryName: String)

  // define AccountFreqLoginMediumType relationship
  case class AccountFreqLoginMediumTypeRaw(accountId: Long, mediumTypeName: String)

  // define AccountHasEmail relationship
  case class AccountHasEmailRaw(accountId: Long, email: String)

  // define CompanyHasUrl relationship
  case class CompanyHasUrlRaw(companyId: Long, url: String)

  // define LoanHasLoanUsage relationship
  case class LoanHasLoanUsageRaw(loanId: Long, loadUsageType: String)

  // define MediumHasMediumType relationship
  case class MediumHasMediumTypeRaw(mediumId: Long, mediumTypeName: String)

  // define MediumHasRiskLevel relationship
  case class MediumHasRiskLevelRaw(mediumId: Long, riskLevelName: String)

  // define Person entity
  case class PersonRaw(
      id: Long,
      createTime: Long,
      name: String,
      isBlocked: Boolean,
      gender: String,
      birthday: Long,
      country: String,
      city: String
  )

  // define Account entity
  case class AccountRaw(
      id: Long,
      createTime: Long,
      deleteTime: Long,
      isBlocked: Boolean,
      `type`: String,
      nickname: String,
      phonenum: String,
      email: String,
      freqLoginType: String,
      lastLoginTime: Long,
      accountLevel: String,
      inDegree: Long,
      OutDegree: Long,
      isExplicitDeleted: Boolean,
      Owner: String
  )

  // define Company entity
  case class CompanyRaw(
      id: Long,
      createTime: Long,
      name: String,
      isBlocked: Boolean,
      country: String,
      city: String,
      business: String,
      description: String,
      url: String
  )

  // define Loan entity
  case class LoanRaw(
      id: Long,
      createTime: Long,
      loanAmount: String,
      balance: String,
      usage: String,
      interestRate: String
  )

  // define Medium entity
  case class MediumRaw(
      id: Long,
      createTime: Long,
      `type`: String,
      isBlocked: Boolean,
      lastLogin: Long,
      riskLevel: String,
  )

  // define PersonApplyLoan relationship
  case class PersonApplyLoanRaw(
      personId: Long,
      loanId: Long,
      loanAmount: String,
      createTime: Long,
      org: String
  )

  // define CompanyApplyLoan relationship
  case class CompanyApplyLoanRaw(
      companyId: Long,
      loanId: Long,
      loanAmount: String,
      createTime: Long,
      org: String
  )

  // define PersonInvestCompany relationship
  case class PersonInvestCompanyRaw(
      investorId: Long,
      companyId: Long,
      createTime: Long,
      ratio: Double
  )

  // define CompanyInvestCompany relationship
  case class CompanyInvestCompanyRaw(
      investorId: Long,
      companyId: Long,
      createTime: Long,
      ratio: Double
  )

  // define PersonGuaranteePerson relationship
  case class PersonGuaranteePersonRaw(
      fromId: Long,
      toId: Long,
      createTime: Long,
      relation: String
  )

  // define CompanyGuarantee relationship
  case class CompanyGuaranteeCompanyRaw(
      fromId: Long,
      toId: Long,
      createTime: Long,
      relation: String
  )

  //define PersonOwnAccount relationship
  case class PersonOwnAccountRaw(
      personId: Long,
      accountId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  )

  // define CompanyOwnAccount relationship
  case class CompanyOwnAccountRaw(
      companyId: Long,
      accountId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean
  )

  // define Transfer relationship
  case class TransferRaw(
      fromId: Long,
      toId: Long,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean,
      orderNum: String,
      comment: String,
      payType: String,
      goodsType: String
  )

  // define Withdraw relationship
  case class WithdrawRaw(
      fromId: Long,
      toId: Long,
      fromType: String,
      toType: String,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean
  )

  // define Repay relationship
  case class RepayRaw(
      accountId: Long,
      loanId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean
  )

  // define Deposit relationship
  case class DepositRaw(
      loanId: Long,
      accountId: Long,
      createTime: Long,
      deleteTime: Long,
      amount: String,
      isExplicitDeleted: Boolean
  )

  // define SignIn relationship
  case class SignInRaw(
      mediumId: Long,
      accountId: Long,
      multiplicityId: Long,
      createTime: Long,
      deleteTime: Long,
      isExplicitDeleted: Boolean,
      location: String
  )
}
