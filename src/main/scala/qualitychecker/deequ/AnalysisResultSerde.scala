package qualitychecker.deequ

import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.{Distribution, Metric}
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultDeserializer, AnalysisResultSerializer, AnalyzerContextDeserializer, AnalyzerContextSerializer, AnalyzerDeserializer, AnalyzerSerializer, DistributionDeserializer, DistributionSerializer, MetricDeserializer, MetricSerializer, ResultKey, ResultKeyDeserializer, ResultKeySerializer}
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import scala.collection.JavaConverters._
import java.util.{ArrayList => JArrayList, HashMap => JHashMap, List => JList, Map => JMap}
import scala.collection.Seq

object AnalysisResultSerde {

  def serialize(analysisResults: Seq[AnalysisResult]): String = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeySerializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultSerializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextSerializer)
      .registerTypeAdapter(classOf[Analyzer[State[_], Metric[_]]],
        AnalyzerSerializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricSerializer)
      .registerTypeAdapter(classOf[Distribution], DistributionSerializer)
      .setPrettyPrinting()
      .create

    gson.toJson(analysisResults.asJava, new TypeToken[JList[AnalysisResult]]() {}.getType)
  }

  def deserialize(analysisResults: String): Seq[AnalysisResult] = {
    val gson = new GsonBuilder()
      .registerTypeAdapter(classOf[ResultKey], ResultKeyDeserializer)
      .registerTypeAdapter(classOf[AnalysisResult], AnalysisResultDeserializer)
      .registerTypeAdapter(classOf[AnalyzerContext], AnalyzerContextDeserializer)
      .registerTypeAdapter(classOf[Analyzer[State[_], Metric[_]]], AnalyzerDeserializer)
      .registerTypeAdapter(classOf[Metric[_]], MetricDeserializer)
      .registerTypeAdapter(classOf[Distribution], DistributionDeserializer)
      .create

    gson.fromJson(analysisResults,
      new TypeToken[JList[AnalysisResult]]() {}.getType)
      .asInstanceOf[JArrayList[AnalysisResult]].asScala
  }
}
