package qualitychecker.deequ

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{AnalysisResult, ResultKey, SimpleResultSerde}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

///**
// * Copied from com.amazon.deequ.repository.AnalysisResult 01/05/2020 as it was private :(
// */
//case class AnalysisResult(
//                           resultKey: ResultKey,
//                           analyzerContext: AnalyzerContext
//                         )

//private[deequ] object AnalysisResult {
//
//  private val DATASET_DATE_FIELD = "dataset_date"
//
//  /**
//   * Get a AnalysisResult as DataFrame containing the success metrics
//   *
//   * @param analysisResult      The AnalysisResult to convert
//   * @param forAnalyzers Only include metrics for these Analyzers in the DataFrame
//   * @param withTags            Only include these Tags in the DataFrame
//   */
//  def getSuccessMetricsAsDataFrame(
//                                    sparkSession: SparkSession,
//                                    analysisResult: AnalysisResult,
//                                    forAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty,
//                                    withTags: Seq[String] = Seq.empty)
//  : DataFrame = {
//
//    var analyzerContextDF = AnalyzerContext
//      .successMetricsAsDataFrame(sparkSession, analysisResult.analyzerContext, forAnalyzers)
//      .withColumn(DATASET_DATE_FIELD, lit(analysisResult.resultKey.dataSetDate))
//
//    analysisResult.resultKey.tags
//      .filterKeys(tagName => withTags.isEmpty || withTags.contains(tagName))
//      .map { case (tagName, tagValue) =>
//        formatTagColumnNameInDataFrame(tagName, analyzerContextDF) -> tagValue}
//      .foreach {
//        case (key, value) => analyzerContextDF = analyzerContextDF.withColumn(key, lit(value))
//      }
//
//    analyzerContextDF
//  }
//
//  /**
//   * Get a AnalysisResult as Json containing the success metrics
//   *
//   * @param analysisResult      The AnalysisResult to convert
//   * @param forAnalyzers Only include metrics for these Analyzers in the DataFrame
//   * @param withTags            Only include these Tags in the DataFrame
//   */
//  def getSuccessMetricsAsJson(
//                               analysisResult: AnalysisResult,
//                               forAnalyzers: Seq[Analyzer[_, Metric[_]]] = Seq.empty,
//                               withTags: Seq[String] = Seq.empty)
//  : String = {
//
//    var serializableResult = SimpleResultSerde.deserialize(
//      AnalyzerContext.successMetricsAsJson(analysisResult.analyzerContext, forAnalyzers))
//      .asInstanceOf[Seq[Map[String, Any]]]
//
//    serializableResult = addColumnToSerializableResult(
//      serializableResult, DATASET_DATE_FIELD, analysisResult.resultKey.dataSetDate)
//
//    analysisResult.resultKey.tags
//      .filterKeys(tagName => withTags.isEmpty || withTags.contains(tagName))
//      .map { case (tagName, tagValue) =>
//        (formatTagColumnNameInJson(tagName, serializableResult), tagValue)}
//      .foreach { case (key, value) => serializableResult = addColumnToSerializableResult(
//        serializableResult, key, value)
//      }
//
//    SimpleResultSerde.serialize(serializableResult)
//  }
//
//  private[this] def addColumnToSerializableResult(
//                                                   serializableResult: Seq[Map[String, Any]],
//                                                   tagName: String,
//                                                   serializableTagValue: Any)
//  : Seq[Map[String, Any]] = {
//
//    if (serializableResult.headOption.nonEmpty &&
//      !serializableResult.head.keySet.contains(tagName)) {
//
//      serializableResult.map {
//        map => map + (tagName -> serializableTagValue)
//      }
//    } else {
//      serializableResult
//    }
//  }
//
//  private[this] def formatTagColumnNameInDataFrame(
//                                                    tagName : String,
//                                                    dataFrame: DataFrame)
//  : String = {
//
//    var tagColumnName = tagName.replaceAll("[^A-Za-z0-9_]", "").toLowerCase
//    if (dataFrame.columns.contains(tagColumnName)) {
//      tagColumnName = tagColumnName + "_2"
//    }
//    tagColumnName
//  }
//
//  private[this] def formatTagColumnNameInJson(
//                                               tagName : String,
//                                               serializableResult : Seq[Map[String, Any]])
//  : String = {
//
//    var tagColumnName = tagName.replaceAll("[^A-Za-z0-9_]", "").toLowerCase
//
//    if (serializableResult.headOption.nonEmpty) {
//      if (serializableResult.head.keySet.contains(tagColumnName)) {
//        tagColumnName = tagColumnName + "_2"
//      }
//    }
//    tagColumnName
//  }
//}
