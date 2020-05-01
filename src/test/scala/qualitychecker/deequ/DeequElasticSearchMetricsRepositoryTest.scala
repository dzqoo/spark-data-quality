package qualitychecker.deequ

import java.time.{LocalDate, ZoneOffset}

import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.metrics.{DoubleMetric, Entity, Metric}
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.sksamuel.elastic4s.testkit.DockerTests
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.wordspec.AnyWordSpec
import qualitychecker.repository.ElasticSearchQcResultsRepository
import utils.CommonFixtures._
import utils.deequ.FixtureSupport

import scala.util.{Failure, Success}

/**
 * Based on InMemoryMetricsRepositoryTest from deequ library on 01/05/2020
 */
class DeequElasticSearchMetricsRepositoryTest extends AnyWordSpec with DatasetSuiteBase with FixtureSupport with DockerTests {
  import spark.implicits._
  private[this] val DATE_ONE = createDate(2017, 10, 14)
  private[this] val DATE_TWO = createDate(2017, 10, 15)
  private[this] val DATE_THREE = createDate(2017, 10, 16)

  private[this] val REGION_EU = Map("Region" -> "EU")
  private[this] val REGION_NA = Map("Region" -> "NA")

  "InMemory Metrics Repository" should {

    "save and retrieve AnalyzerContexts" in {
      evaluate(spark) { (results, repository) =>

        val resultKey = ResultKey(DATE_ONE, REGION_EU)
        repository.save(resultKey, results)

        val loadedResults = repository.loadByKey(resultKey).get

        val loadedResultsAsDataFrame = successMetricsAsDataFrame(spark, loadedResults)
        val resultsAsDataFrame = successMetricsAsDataFrame(spark, results)

        assertSameRows(loadedResultsAsDataFrame, resultsAsDataFrame)
        assert(results == loadedResults)
      }
    }

    "save should ignore failed result metrics when saving" in {

      val metrics: Map[Analyzer[_, Metric[_]], Metric[_]] = Map(
        Size() -> DoubleMetric(Entity.Column, "Size", "*", Success(5.0)),
        Completeness("ColumnA") ->
          DoubleMetric(Entity.Column, "Completeness", "ColumnA",
            Failure(new RuntimeException("error"))))

      val resultsWithMixedValues = AnalyzerContext(metrics)

      val successMetrics = resultsWithMixedValues.metricMap
        .filter { case (_, metric) => metric.value.isSuccess }

      val resultsWithSuccessfulValues = AnalyzerContext(successMetrics)

      val repository = createRepository()

      val resultKey = ResultKey(DATE_ONE, REGION_EU)

      repository.save(resultKey, resultsWithMixedValues)

      val loadedAnalyzerContext = repository.loadByKey(resultKey).get

      assert(resultsWithSuccessfulValues == loadedAnalyzerContext)
    }

    "save and retrieve AnalysisResults" in {

      evaluate(spark) { (results, repository) =>

        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
        repository.save(ResultKey(DATE_TWO, REGION_NA), results)

        val analysisResultsAsDataFrame = repository.load()
          .after(DATE_ONE)
          .getSuccessMetricsAsDataFrame(spark)

        import spark.implicits._
        val expected = Seq(
          // First analysisResult
          ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
          ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"),
          // Second analysisResult
          ("Dataset", "*", "Size", 4.0, DATE_TWO, "NA"),
          ("Column", "item", "Distinctness", 1.0, DATE_TWO, "NA"),
          ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "only load AnalysisResults within a specific time frame if requested" in {
      evaluate(spark) { (results, repository) =>

        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
        repository.save(ResultKey(DATE_TWO, REGION_NA), results)
        repository.save(ResultKey(DATE_THREE, REGION_NA), results)

        val analysisResultsAsDataFrame = repository.load()
          .after(DATE_TWO)
          .before(DATE_TWO)
          .getSuccessMetricsAsDataFrame(spark)
        val expected = Seq(
          // Second analysisResult
          ("Dataset", "*", "Size", 4.0, DATE_TWO, "NA"),
          ("Column", "item", "Distinctness", 1.0, DATE_TWO, "NA"),
          ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "only load AnalyzerContexts with specific Tags if requested" in {

      evaluate(spark) { (results, repository) =>

        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
        repository.save(ResultKey(DATE_TWO, REGION_NA), results)

        val analysisResultsAsDataframe = repository.load()
          .after(DATE_ONE)
          .withTagValues(REGION_EU)
          .getSuccessMetricsAsDataFrame(spark)

        import spark.implicits._
        val expected = Seq(
          // First analysisResult
          ("Dataset", "*", "Size", 4.0, DATE_ONE, "EU"),
          ("Column", "item", "Distinctness", 1.0, DATE_ONE, "EU"),
          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataframe, expected)
      }
    }

    "only include specifics metrics in loaded AnalysisResults if requested" in {
      evaluate(spark) { (results, repository) =>

        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
        repository.save(ResultKey(DATE_TWO, REGION_NA), results)

        val analysisResultsAsDataFrame = repository.load()
          .after(DATE_ONE)
          .forAnalyzers(Seq(Completeness("att1"), Uniqueness(Seq("att1", "att2"))))
          .getSuccessMetricsAsDataFrame(spark)
        val expected = Seq(
          // First analysisResult
          ("Column", "att1", "Completeness", 1.0, DATE_ONE, "EU"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_ONE, "EU"),
          // Second analysisResult
          ("Column", "att1", "Completeness", 1.0, DATE_TWO, "NA"),
          ("Mutlicolumn", "att1,att2", "Uniqueness", 0.25, DATE_TWO, "NA"))
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "include no metrics in loaded AnalysisResults if requested" in {

      evaluate(spark) { (results, repository) =>

        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
        repository.save(ResultKey(DATE_TWO, REGION_NA), results)

        val analysisResultsAsDataFrame = repository.load()
          .after(DATE_ONE)
          .forAnalyzers(Seq.empty)
          .getSuccessMetricsAsDataFrame(spark)

        import spark.implicits._
        val expected = Seq.empty[(String, String, String, Double, Long, String)]
          .toDF("entity", "instance", "name", "value", "dataset_date", "region")

        assertSameRows(analysisResultsAsDataFrame, expected)
      }
    }

    "return empty Seq if load parameters too restrictive" in {

      evaluate(spark) { (results, repository) =>

        repository.save(ResultKey(DATE_ONE, REGION_EU), results)
        repository.save(ResultKey(DATE_TWO, REGION_NA), results)

        val analysisResults = repository.load()
          .after(DATE_TWO)
          .before(DATE_ONE)
          .get()

        assert(analysisResults.isEmpty)
      }
    }
  }

  private[this] def evaluate(session: SparkSession)
                            (test: (AnalyzerContext, MetricsRepository) => Unit): Unit = {

    val data = getDfFull(session)

    val results = createAnalysis().run(data)

    val repository = createRepository()

    test(results, repository)
  }

  private[this] def createAnalysis(): Analysis = {
    Analysis()
      .addAnalyzer(Size())
      .addAnalyzer(Distinctness("item"))
      .addAnalyzer(Completeness("att1"))
      .addAnalyzer(Uniqueness(Seq("att1", "att2")))
  }

  private[this] def createDate(year: Int, month: Int, day: Int): Long = {
    LocalDate.of(year, month, day).atTime(10, 10, 10).toEpochSecond(ZoneOffset.UTC)
  }

  private[this] def createRepository(): MetricsRepository = {
    new DeequElasticSearchMetricsRepository(client, someIndex)
  }

  private[this] def assertSameRows(dataFrameA: DataFrame, dataFrameB: DataFrame): Unit = {
    assert(dataFrameA.collect().toSet == dataFrameB.collect().toSet)
  }
}
