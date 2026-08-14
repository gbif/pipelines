package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.HumboldtExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Migration oracle for Humboldt behaviour already implemented by the legacy builder. New terms such
 * as survey-agent-role are intentionally outside these parity tests until the legacy mapping is fully
 * retired.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HumboldtMappingParityTest {
  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("HumboldtMappingParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void usesSameHumboldtRowTypeAsLegacyBuilder() {
    assertEquals(HumboldtExtensionBuilder.ROW_TYPE_HUMBOLDT, HumboldtMapping.ROW_TYPE_HUMBOLDT);
  }

  @Test
  void surveyTargetsAndDirectFieldsMatchLegacyBuilder() {
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "survey", survey(),
            "survey-survey-target", surveyTargetLinks(),
            "survey-target", surveyTargets());

    List<Map<String, String>> legacy = legacyHumboldt(loader);
    List<Map<String, String>> mapped = mappedHumboldt(loader);

    assertEquals(2, legacy.size());
    assertEquals(legacy.size(), mapped.size());

    String siteCount = TermResolver.resolve("siteCount");
    String reportedWeather = TermResolver.resolve("reportedWeather");
    String targetDescription = TermResolver.resolve("surveyTargetDescription");

    Comparator<Map<String, String>> byTarget =
        Comparator.comparing(
            row -> row.get(targetDescription), Comparator.nullsFirst(String::compareTo));
    legacy = legacy.stream().sorted(byTarget).toList();
    mapped = mapped.stream().sorted(byTarget).toList();

    for (int i = 0; i < legacy.size(); i++) {
      assertEquals(legacy.get(i).get(siteCount), mapped.get(i).get(siteCount));
      assertEquals(legacy.get(i).get(reportedWeather), mapped.get(i).get(reportedWeather));
      assertEquals(legacy.get(i).get(targetDescription), mapped.get(i).get(targetDescription));
    }
  }

  @Test
  void surveyWithoutTargetTablesStillProducesOneHumboldtRow() {
    TableLoader loader = TestTableLoader.of("event", event(), "survey", survey());

    List<Map<String, String>> legacy = legacyHumboldt(loader);
    List<Map<String, String>> mapped = mappedHumboldt(loader);

    assertEquals(1, legacy.size());
    assertEquals(legacy.size(), mapped.size());
    assertEquals(
        legacy.get(0).get(TermResolver.resolve("siteCount")),
        mapped.get(0).get(TermResolver.resolve("siteCount")));
    assertEquals(
        legacy.get(0).get(TermResolver.resolve("reportedWeather")),
        mapped.get(0).get(TermResolver.resolve("reportedWeather")));
  }

  @Test
  void presentTargetTablesWithNoLinksStillPreserveSurveyRow() {
    Dataset<Row> emptyLinks =
        spark.createDataFrame(
            List.of(),
            new StructType()
                .add("survey_fk", DataTypes.StringType)
                .add("surveyTarget_fk", DataTypes.StringType));
    Dataset<Row> emptyTargets =
        spark.createDataFrame(
            List.of(),
            new StructType()
                .add("surveyTarget_pk", DataTypes.StringType)
                .add("surveyTargetDescription", DataTypes.StringType));
    TableLoader loader =
        TestTableLoader.of(
            "event", event(),
            "survey", survey(),
            "survey-survey-target", emptyLinks,
            "survey-target", emptyTargets);

    List<Map<String, String>> legacy = legacyHumboldt(loader);
    List<Map<String, String>> mapped = mappedHumboldt(loader);

    assertEquals(1, legacy.size());
    assertEquals(legacy.size(), mapped.size());
    assertEquals(
        legacy.get(0).get(TermResolver.resolve("siteCount")),
        mapped.get(0).get(TermResolver.resolve("siteCount")));
  }

  @Test
  void multipleSurveysForSameEventRemainIndependentRows() {
    Dataset<Row> surveys =
        spark.createDataFrame(
            List.of(
                RowFactory.create("SPK-001", "EPK-001", "3", "Clear"),
                RowFactory.create("SPK-002", "EPK-001", "7", "Rain")),
            new StructType()
                .add("survey_pk", DataTypes.StringType)
                .add("event_fk", DataTypes.StringType)
                .add("siteCount", DataTypes.StringType)
                .add("reportedWeather", DataTypes.StringType));
    TableLoader loader = TestTableLoader.of("event", event(), "survey", surveys);

    List<Map<String, String>> legacy = legacyHumboldt(loader);
    List<Map<String, String>> mapped = mappedHumboldt(loader);

    assertEquals(2, legacy.size());
    assertEquals(legacy.size(), mapped.size());

    String siteCount = TermResolver.resolve("siteCount");
    String reportedWeather = TermResolver.resolve("reportedWeather");
    Comparator<Map<String, String>> bySiteCount =
        Comparator.comparing(row -> row.get(siteCount), Comparator.nullsFirst(String::compareTo));
    legacy = legacy.stream().sorted(bySiteCount).toList();
    mapped = mapped.stream().sorted(bySiteCount).toList();

    for (int i = 0; i < legacy.size(); i++) {
      assertEquals(legacy.get(i).get(siteCount), mapped.get(i).get(siteCount));
      assertEquals(legacy.get(i).get(reportedWeather), mapped.get(i).get(reportedWeather));
    }
  }

  @Test
  void protocolFkResolutionAndPublisherPrecedenceMatchLegacyBuilder() {
    Dataset<Row> survey =
        spark.createDataFrame(
            List.of(
                RowFactory.create("SPK-001", "EPK-001", null, "P-001", null, "P-002"),
                RowFactory.create(
                    "SPK-002", "EPK-001", "Publisher protocol", "P-001", "Publisher effort", "P-002")),
            new StructType()
                .add("survey_pk", DataTypes.StringType)
                .add("event_fk", DataTypes.StringType)
                .add("samplingProtocol", DataTypes.StringType)
                .add("samplingProtocol_fk", DataTypes.StringType)
                .add("samplingEffortProtocol", DataTypes.StringType)
                .add("samplingEffortProtocol_fk", DataTypes.StringType));
    Dataset<Row> protocol =
        spark.createDataFrame(
            List.of(
                RowFactory.create("P-001", "Resolved protocol"),
                RowFactory.create("P-002", "Resolved effort")),
            new StructType()
                .add("protocol_pk", DataTypes.StringType)
                .add("protocolDescription", DataTypes.StringType));

    TableLoader loader = TestTableLoader.of("event", event(), "survey", survey, "protocol", protocol);

    List<Map<String, String>> legacy = legacyHumboldt(loader);
    List<Map<String, String>> mapped = mappedHumboldt(loader);

    assertEquals(2, legacy.size());
    assertEquals(2, mapped.size());

    Map<String, String> mappedResolved =
        mapped.stream()
            .filter(
                row ->
                    "Resolved protocol"
                        .equals(row.get(EcoTerm.protocolDescriptions.qualifiedName())))
            .findFirst()
            .orElseThrow();
    Map<String, String> legacyResolved =
        legacy.stream()
            .filter(
                row ->
                    "Resolved protocol"
                        .equals(row.get(EcoTerm.protocolDescriptions.qualifiedName())))
            .findFirst()
            .orElseThrow();
    assertEquals(
        legacyResolved.get(EcoTerm.samplingEffortProtocol.qualifiedName()),
        mappedResolved.get(EcoTerm.samplingEffortProtocol.qualifiedName()));
    assertNull(mappedResolved.get(DwcTerm.samplingProtocol.qualifiedName()));

    Map<String, String> mappedPublisher =
        mapped.stream()
            .filter(
                row ->
                    "Publisher protocol"
                        .equals(row.get(EcoTerm.protocolDescriptions.qualifiedName())))
            .findFirst()
            .orElseThrow();
    Map<String, String> legacyPublisher =
        legacy.stream()
            .filter(
                row ->
                    "Publisher protocol"
                        .equals(row.get(EcoTerm.protocolDescriptions.qualifiedName())))
            .findFirst()
            .orElseThrow();
    assertEquals(
        legacyPublisher.get(EcoTerm.samplingEffortProtocol.qualifiedName()),
        mappedPublisher.get(EcoTerm.samplingEffortProtocol.qualifiedName()));
  }

  @Test
  void publisherProtocolValuesSurviveWhenProtocolLookupTableIsAbsent() {
    Dataset<Row> survey =
        spark.createDataFrame(
            List.of(
                RowFactory.create(
                    "SPK-001", "EPK-001", "Publisher protocol", "P-001", "Publisher effort", "P-002")),
            new StructType()
                .add("survey_pk", DataTypes.StringType)
                .add("event_fk", DataTypes.StringType)
                .add("samplingProtocol", DataTypes.StringType)
                .add("samplingProtocol_fk", DataTypes.StringType)
                .add("samplingEffortProtocol", DataTypes.StringType)
                .add("samplingEffortProtocol_fk", DataTypes.StringType));

    TableLoader loader = TestTableLoader.of("event", event(), "survey", survey);

    List<Map<String, String>> legacy = legacyHumboldt(loader);
    List<Map<String, String>> mapped = mappedHumboldt(loader);

    assertEquals(1, legacy.size());
    assertEquals(1, mapped.size());
    assertEquals(
        legacy.get(0).get(EcoTerm.protocolDescriptions.qualifiedName()),
        mapped.get(0).get(EcoTerm.protocolDescriptions.qualifiedName()));
    assertEquals(
        legacy.get(0).get(EcoTerm.samplingEffortProtocol.qualifiedName()),
        mapped.get(0).get(EcoTerm.samplingEffortProtocol.qualifiedName()));
  }

  @Test
  void absentSurveyMeansAbsentHumboldtExtensionRatherThanFailedCoreConversion() {
    TableLoader loader = TestTableLoader.of("event", event());

    List<ExtendedRecord> legacy = EventCoreBuilder.build(spark, loader).collectAsList();
    List<ExtendedRecord> mapped =
        new DwcDpMappingEngine(graph)
            .execute(loader, EventDwcaMapping.withHumboldt(graph))
            .collectAsList();

    assertEquals(1, legacy.size());
    assertEquals(1, mapped.size());
    assertFalse(legacy.get(0).getExtensions().containsKey(HumboldtExtensionBuilder.ROW_TYPE_HUMBOLDT));
    assertFalse(mapped.get(0).getExtensions().containsKey(HumboldtMapping.ROW_TYPE_HUMBOLDT));
  }

  private List<Map<String, String>> legacyHumboldt(TableLoader loader) {
    List<ExtendedRecord> records = EventCoreBuilder.build(spark, loader).collectAsList();
    assertEquals(1, records.size());
    List<Map<String, String>> rows =
        records.get(0).getExtensions().get(HumboldtExtensionBuilder.ROW_TYPE_HUMBOLDT);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> mappedHumboldt(TableLoader loader) {
    List<ExtendedRecord> records =
        new DwcDpMappingEngine(graph)
            .execute(loader, EventDwcaMapping.withHumboldt(graph))
            .collectAsList();
    assertEquals(1, records.size());
    List<Map<String, String>> rows =
        records.get(0).getExtensions().get(HumboldtMapping.ROW_TYPE_HUMBOLDT);
    assertNotNull(rows);
    return rows;
  }

  private Dataset<Row> event() {
    return spark.createDataFrame(
        List.of(RowFactory.create("EPK-001", "EVT001")),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType));
  }

  private Dataset<Row> survey() {
    return spark.createDataFrame(
        List.of(RowFactory.create("SPK-001", "EPK-001", "3", "Clear")),
        new StructType()
            .add("survey_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("siteCount", DataTypes.StringType)
            .add("reportedWeather", DataTypes.StringType));
  }

  private Dataset<Row> surveyTargetLinks() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create("SPK-001", "STP-001"),
            RowFactory.create("SPK-001", "STP-002")),
        new StructType()
            .add("survey_fk", DataTypes.StringType)
            .add("surveyTarget_fk", DataTypes.StringType));
  }

  private Dataset<Row> surveyTargets() {
    return spark.createDataFrame(
        List.of(
            RowFactory.create("STP-001", "All birds"),
            RowFactory.create("STP-002", "All mammals")),
        new StructType()
            .add("surveyTarget_pk", DataTypes.StringType)
            .add("surveyTargetDescription", DataTypes.StringType));
  }
}
