package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.MixsTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.EventCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledExtension;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EventNucleotideMappingParityTest {

  private SparkSession spark;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("EventNucleotideMappingParityTest").getOrCreate();
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = EventDwcaMapping.withNucleotide(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void eventOnlyAnalysisSequenceAndProtocolMatchLegacy() {
    TableLoader loader =
        loader(
            analyses(RowFactory.create("NAPK-1", "EPK-1", null, "MPPK-1", "NSPK-1", 42)),
            sequences(RowFactory.create("NSPK-1", "NS-1", "GGCCTTAA", "clean")),
            protocols(RowFactory.create("MPPK-1", "COI", "metabarcoding")));

    List<Map<String, String>> legacy = dna(only(EventCoreBuilder.build(spark, loader).collectAsList()));
    List<Map<String, String>> mapped = dna(only(engine.execute(loader, plan).collectAsList()));

    assertEquals(normalizeLegacy(legacy), mapped);
    assertEquals("GGCCTTAA", mapped.get(0).get(GbifDnaTerm.dna_sequence.qualifiedName()));
    assertEquals("COI", mapped.get(0).get(MixsTerm.target_gene.qualifiedName()));
    assertEquals("42", mapped.get(0).get(TargetTerms.resolve("readCount")));
  }

  @Test
  void analysisWithMaterialOwnershipIsNotDuplicatedOntoEvent() {
    TableLoader loader =
        loader(
            analyses(RowFactory.create("NAPK-1", "EPK-1", "MEPK-1", "MPPK-1", "NSPK-1", 42)),
            sequences(RowFactory.create("NSPK-1", "NS-1", "GGCCTTAA", null)),
            protocols(RowFactory.create("MPPK-1", "COI", null)));

    ExtendedRecord legacy = only(EventCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(legacy.getExtensions().containsKey(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA));
    assertFalse(mapped.getExtensions().containsKey(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA));
  }

  @Test
  void governingEventDnaFragmentsUseEventOwnershipAndAnalysisRowIdentity() {
    CompiledExtension extension =
        engine.compile(plan).extensions().stream()
            .filter(e -> e.rowType().equals(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA))
            .findFirst()
            .orElseThrow();

    assertEquals(3, extension.fragments().size());

    var base =
        extension.fragments().stream()
            .filter(f -> f.name().equals("event-nucleotide-analysis"))
            .findFirst()
            .orElseThrow();
    assertEquals("event_pk", base.scopeKey().column());
    assertEquals("event_fk", base.path().relations().get(0).targetColumn());
    assertEquals("nucleotideAnalysis_pk", base.rowIdentity().orElseThrow().column());
    assertTrue(base.relations().get(0).filter().isPresent());

    var sequence =
        extension.fragments().stream()
            .filter(f -> f.name().equals("event-nucleotide-sequence"))
            .findFirst()
            .orElseThrow();
    assertEquals("nucleotideAnalysis_pk", sequence.rowMatch().orElseThrow().column());

    var protocol =
        extension.fragments().stream()
            .filter(f -> f.name().equals("event-molecular-protocol"))
            .findFirst()
            .orElseThrow();
    assertEquals("nucleotideAnalysis_pk", protocol.rowMatch().orElseThrow().column());

    long dnaSequenceProducers =
        extension.fragments().stream()
            .flatMap(f -> f.targets().stream())
            .filter(f -> f.targetTerm().equals(GbifDnaTerm.dna_sequence.qualifiedName()))
            .count();
    assertEquals(1, dnaSequenceProducers);
  }

  private TableLoader loader(
      Dataset<Row> analyses, Dataset<Row> sequences, Dataset<Row> protocols) {
    return TestTableLoader.of(
        "event", event(),
        "nucleotide-analysis", analyses,
        "nucleotide-sequence", sequences,
        "molecular-protocol", protocols);
  }

  private Dataset<Row> event() {
    return spark.createDataFrame(
        List.of(RowFactory.create("EPK-1", "EVT001")),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventID", DataTypes.StringType));
  }

  private Dataset<Row> analyses(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("nucleotideAnalysis_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("molecularProtocol_fk", DataTypes.StringType)
            .add("nucleotideSequence_fk", DataTypes.StringType)
            .add("readCount", DataTypes.IntegerType));
  }

  private Dataset<Row> sequences(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("nucleotideSequence_pk", DataTypes.StringType)
            .add("nucleotideSequenceID", DataTypes.StringType)
            .add("sequence", DataTypes.StringType)
            .add("nucleotideSequenceRemarks", DataTypes.StringType));
  }

  private Dataset<Row> protocols(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("molecularProtocol_pk", DataTypes.StringType)
            .add("target_gene", DataTypes.StringType)
            .add("pcr_analysis_software", DataTypes.StringType));
  }

  private List<Map<String, String>> dna(ExtendedRecord record) {
    List<Map<String, String>> rows =
        record.getExtensions().get(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> normalizeLegacy(List<Map<String, String>> rows) {
    String eventId = TargetTerms.resolve("eventID");
    return rows.stream()
        .map(
            row -> {
              Map<String, String> normalized = new LinkedHashMap<>(row);
              normalized.remove(eventId);
              return normalized;
            })
        .toList();
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
