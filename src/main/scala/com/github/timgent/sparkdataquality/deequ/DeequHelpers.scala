package com.github.timgent.sparkdataquality.deequ

import java.time.Instant

import com.amazon.deequ.VerificationResult
import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType}
import com.github.timgent.sparkdataquality.checkssuite.CheckSuiteStatus.{Success, Warning}
import com.github.timgent.sparkdataquality.checkssuite.{CheckSuiteStatus, ChecksSuiteResult}
import com.github.timgent.sparkdataquality.sparkdataquality.DeequCheckStatus

object DeequHelpers {
  implicit private[sparkdataquality] class VerificationResultExtension(
      verificationResult: VerificationResult
  ) {
    def toCheckSuiteResult(
        description: String,
        timestamp: Instant,
        checkTags: Map[String, String]
    ): ChecksSuiteResult = {
      val checkStatus = verificationResult.status match {
        case com.amazon.deequ.checks.CheckStatus.Success => Success
        case com.amazon.deequ.checks.CheckStatus.Warning => Warning
        case com.amazon.deequ.checks.CheckStatus.Error   => CheckSuiteStatus.Error
      }
      val checkResults = verificationResult.checkResults.map {
        case (deequCheck, deequCheckResult) =>
          val checkResultDescription = deequCheckResult.status match {
            case com.amazon.deequ.checks.CheckStatus.Success => "Deequ check was successful"
            case com.amazon.deequ.checks.CheckStatus.Warning => "Deequ check produced a warning"
            case com.amazon.deequ.checks.CheckStatus.Error   => "Deequ check produced an error"
          }
          CheckResult(
            QcType.DeequQualityCheck,
            deequCheckResult.status.toCheckStatus,
            checkResultDescription,
            deequCheck.description
          )
      }.toSeq
      ChecksSuiteResult( // Do we want to add deequ constraint results to the checks suite result too? It's another level compared to what we have elsewhere. Could refactor to match deequ's way of doing things
        checkStatus,
        description,
        checkResults,
        timestamp,
        checkTags
      )
    }
    def toCheckResults(checkTags: Map[String, String]): Seq[CheckResult] = {
      verificationResult.checkResults.map {
        case (deequCheck, deequCheckResult) =>
          val checkResultDescription = deequCheckResult.status match {
            case com.amazon.deequ.checks.CheckStatus.Success => "Deequ check was successful"
            case com.amazon.deequ.checks.CheckStatus.Warning => "Deequ check produced a warning"
            case com.amazon.deequ.checks.CheckStatus.Error   => "Deequ check produced an error"
          }
          CheckResult(
            QcType.DeequQualityCheck,
            deequCheckResult.status.toCheckStatus,
            checkResultDescription,
            deequCheck.description
          )
      }.toSeq
    }
  }

  implicit private[sparkdataquality] class DeequCheckStatusEnricher(checkStatus: DeequCheckStatus) {
    def toCheckStatus =
      checkStatus match {
        case DeequCheckStatus.Success => CheckStatus.Success
        case DeequCheckStatus.Warning => CheckStatus.Warning
        case DeequCheckStatus.Error   => CheckStatus.Error
      }
  }
}
