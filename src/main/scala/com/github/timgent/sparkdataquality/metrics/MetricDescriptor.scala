package com.github.timgent.sparkdataquality.metrics

import com.github.timgent.sparkdataquality.metrics.MetricCalculator.{
  ComplianceMetricCalculator,
  DistinctValuesMetricCalculator,
  SizeMetricCalculator,
  SumValuesMetricCalculator
}
import com.github.timgent.sparkdataquality.metrics.MetricValue.NumericMetricValue

/**
  * Describes the metric being calculated
  */
private[sparkdataquality] trait MetricDescriptor {
  type MC <: MetricCalculator

  /**
    * The metricCalculator which contains key logic for calculating a MetricValue for this MetricDescriptor
    * @return the MetricCalculator
    */
  def metricCalculator: MC

  /**
    * A representation of the MetricDescriptor that can be more easily handled for persistence
    * @return the SimpleMetricDescriptor
    */
  def toSimpleMetricDescriptor: SimpleMetricDescriptor

  /**
    * A name for the metric
    * @return
    */
  def metricName: String
}

object MetricDescriptor {

  /**
    * A MetricDescriptor which can have the dataset filtered before the metric is calculated
    */
  trait Filterable {

    /**
      * A filter to apply before calculation of the metric
      * @return the MetricFilter
      */
    def filter: MetricFilter
  }

  /**
    * A metric that calculates the number of rows in your dataset
    * @param filter - filter to be applied before the size is calculated
    */
  case class SizeMetricDescriptor(filter: MetricFilter = MetricFilter.noFilter) extends MetricDescriptor with Filterable {
    override def metricCalculator: SizeMetricCalculator = SizeMetricCalculator(filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(metricName, Some(filter.filterDescription))
    override def metricName: String = "Size"
    override type MC = SizeMetricCalculator
  }

  /**
    * A metric that calculates the number of rows in your dataset
    * @param filter - filter to be applied before the size is calculated
    */
  case class SumValuesMetricDescriptor[MV <: NumericMetricValue: MetricValueConstructor](
      onColumn: String,
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: SumValuesMetricCalculator[MV] = SumValuesMetricCalculator[MV](onColumn, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(metricName, Some(filter.filterDescription))
    override def metricName: String = "SumValues"
    override type MC = SumValuesMetricCalculator[MV]
  }

  /**
    * A metric that calculates what fraction of rows comply with the given criteria
    * @param complianceFn - the criteria used to check each rows compliance
    * @param filter - a filter to be applied before the compliance fraction is calculated
    */
  case class ComplianceMetricDescriptor(
      complianceFn: ComplianceFn,
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: ComplianceMetricCalculator =
      ComplianceMetricCalculator(complianceFn, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(
        metricName,
        Some(filter.filterDescription),
        Some(complianceFn.description)
      )
    override def metricName: String = "Compliance"
    override type MC = ComplianceMetricCalculator
  }

  /**
    * A metric that calculates the number of distinct values in a column or across several columns
    * @param onColumns - the columns for which you are counting distinct values
    * @param filter - the filter to be applied before the distinct count is calculated
    */
  case class DistinctValuesMetricDescriptor(
      onColumns: List[String],
      filter: MetricFilter = MetricFilter.noFilter
  ) extends MetricDescriptor
      with Filterable {
    override def metricCalculator: DistinctValuesMetricCalculator =
      DistinctValuesMetricCalculator(onColumns, filter)
    override def toSimpleMetricDescriptor: SimpleMetricDescriptor =
      SimpleMetricDescriptor(
        metricName,
        Some(filter.filterDescription),
        onColumns = Some(onColumns)
      )
    override def metricName: String = "DistinctValues"
    override type MC = DistinctValuesMetricCalculator
  }
}

/**
  * Representation of a MetricDescriptor which is easy to persist
  */
private[sparkdataquality] case class SimpleMetricDescriptor(
    metricName: String,
    filterDescription: Option[String] = None,
    complianceDescription: Option[String] = None,
    onColumns: Option[List[String]] = None
)
