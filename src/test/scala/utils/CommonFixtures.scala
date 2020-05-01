package utils

import java.time.Instant

import qualitychecker.checks.QCCheck.SingleDatasetCheck
import qualitychecker.checks.{CheckStatus, RawCheckResult}

object CommonFixtures {
  val now = Instant.now
  val someCheck = SingleDatasetCheck("some check")(_ => RawCheckResult(CheckStatus.Success, "successful"))
  val someTags = Map("project" -> "project A")
  val someIndex = "index_name"
}
