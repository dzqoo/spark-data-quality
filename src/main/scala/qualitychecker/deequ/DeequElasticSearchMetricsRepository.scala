package qualitychecker.deequ

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.{AnalysisResultSerde, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import com.sksamuel.elastic4s.{ElasticClient, Index}
import com.amazon.deequ.repository.AnalysisResult
//import qualitychecker.deequ.AnalysisResult

class DeequElasticSearchMetricsRepository(client: ElasticClient, index: Index) extends MetricsRepository {


  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)

    val serializedResult = AnalysisResultSerde.serialize(
      AnalysisResult(resultKey, analyzerContextWithSuccessfulValues)
    )

//    FileSystemMetricsRepository.writeToFileOnDfs(spark, path, {
//      val bytes = serializedResult.getBytes(FileSystemMetricsRepository.CHARSET_NAME)
//      _.write(bytes)
//    })
    ???
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = ???

  override def load(): MetricsRepositoryMultipleResultsLoader = ???
}

class DeequElasticSearchMetricsRespositoryMultipleResultsLoader extends MetricsRepositoryMultipleResultsLoader {
  override def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = ???

  override def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): MetricsRepositoryMultipleResultsLoader = ???

  override def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = ???

  override def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = ???

  override def get(): Seq[repository.AnalysisResult] = Seq.empty
}