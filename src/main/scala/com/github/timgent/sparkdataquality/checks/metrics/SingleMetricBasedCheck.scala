package com.github.timgent.sparkdataquality.checks.metrics

import com.github.timgent.sparkdataquality.checks.{CheckResult, CheckStatus, QcType, RawCheckResult}
import com.github.timgent.sparkdataquality.metrics.MetricDescriptor.{
  ComplianceMetricDescriptor,
  DistinctValuesMetricDescriptor,
  SizeMetricDescriptor
}
import com.github.timgent.sparkdataquality.metrics.MetricValue.{DoubleMetric, LongMetric}
import com.github.timgent.sparkdataquality.metrics.{ComplianceFn, MetricDescriptor, MetricFilter, MetricValue}
import com.github.timgent.sparkdataquality.thresholds.AbsoluteThreshold

import scala.reflect.ClassTag

/**
  * A check based on a single metric
  * @param metricDescriptor - describes the metric the check will be done on
  * @param description - the user friendly description for this check
  * @param check - the check to be done
  * @tparam MV - the type of the MetricValue that will be calculated in order to complete this check
  */
case class SingleMetricBasedCheck[MV <: MetricValue](metricDescriptor: MetricDescriptor, description: String)(
    check: MV => RawCheckResult
) extends MetricsBasedCheck {
  def applyCheck(metric: MV): CheckResult = {
    check(metric).withDescription(QcType.MetricsBasedQualityCheck, description)
  }

  // typeTag required here to enable match of metric on type MV. Without class tag this type check would be fruitless
  private[sparkdataquality] final def applyCheckOnMetrics(
      metrics: Map[MetricDescriptor, MetricValue]
  )(implicit classTag: ClassTag[MV]): CheckResult = {
    val metricOfInterestOpt: Option[MetricValue] =
      metrics.get(metricDescriptor).map(metricValue => metricValue)
    metricOfInterestOpt match {
      case Some(metric) =>
        metric match { // TODO: Look into heterogenous maps to avoid this type test - https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-1.2.4#heterogenous-maps
          case metric: MV => applyCheck(metric)
          case _          => metricTypeErrorResult
        }
      case None => metricNotPresentErrorResult
    }
  }
}

object SingleMetricBasedCheck {

  /**
    * A check based on a single metric that checks if that metric is within the given threshold
    * @param metricDescriptor - describes the metric the check will be done on
    * @param description - the user friendly description for this check
    * @param threshold - the threshold that the metric must be within to pass
    * @tparam MV - the type of the MetricValue that will be calculated in order to complete this check
    * @return
    */
  def thresholdBasedCheck[MV <: MetricValue](
      metricDescriptor: MetricDescriptor,
      description: String,
      threshold: AbsoluteThreshold[MV#T]
  ): SingleMetricBasedCheck[MV] = {
    SingleMetricBasedCheck(metricDescriptor, description) { metricValue: MV =>
      if (threshold.isWithinThreshold(metricValue.value)) {
        RawCheckResult(
          CheckStatus.Success,
          s"${metricDescriptor.metricName} of ${metricValue.value} was within the range $threshold"
        )
      } else {
        RawCheckResult(
          CheckStatus.Error,
          s"${metricDescriptor.metricName} of ${metricValue.value} was outside the range $threshold"
        )
      }
    }
  }

  /**
    * Checks the count of rows in a dataset after the given filter is applied is within the given threshold
    * @param threshold
    * @param filter - filter to be applied before rows are counted
    */
  def sizeCheck(threshold: AbsoluteThreshold[Long], filter: MetricFilter = MetricFilter.noFilter): SingleMetricBasedCheck[LongMetric] =
    thresholdBasedCheck[LongMetric](SizeMetricDescriptor(filter), s"SizeCheck with filter: ${filter.filterDescription}", threshold)

  /**
    * Checks the fraction of rows that are compliant with the given complianceFn
    * @param threshold - the threshold for what fraction of rows is acceptable
    * @param complianceFn - the function rows are tested with to see if they are compliant
    * @param filter - the filter that is applied before the compliance fraction is calculated
    */
  def complianceCheck(
      threshold: AbsoluteThreshold[Double],
      complianceFn: ComplianceFn,
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricBasedCheck[DoubleMetric] =
    thresholdBasedCheck[DoubleMetric](
      ComplianceMetricDescriptor(complianceFn, filter),
      s"ComplianceCheck ${complianceFn.description} with filter: ${filter.filterDescription}",
      threshold
    )

  /**
    * Checks the number of distinct values across the given columns
    * @param threshold - the threshold for what number of distinct values is acceptable
    * @param onColumns - the columns to check for distinct values in
    * @param filter - the filter that is applied before the distinct value count is done
    */
  def distinctValuesCheck(
      threshold: AbsoluteThreshold[Long],
      onColumns: List[String],
      filter: MetricFilter = MetricFilter.noFilter
  ): SingleMetricBasedCheck[LongMetric] =
    thresholdBasedCheck[LongMetric](
      DistinctValuesMetricDescriptor(onColumns, filter),
      s"DistinctValuesCheck on columns: $onColumns with filter: ${filter.filterDescription}",
      threshold
    )
}
