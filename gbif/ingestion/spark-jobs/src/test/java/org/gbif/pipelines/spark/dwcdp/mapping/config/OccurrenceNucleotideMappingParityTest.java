package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Comparator;
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
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
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
class OccurrenceNucleotideMappingParityTest {

  private SparkSession spark;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder()
            .appName("OccurrenceNucleotideMappingParityTest")
            .getOrCreate();
    SchemaGraph graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = OccurrenceDwcaMapping.withNucleotide(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void materialAnalysisSequenceAndProtocolMatchLegacy() {
    TableLoader loader =
        loader(
            material("MEPK-1", "OCC001"),
            analyses(RowFactory.create("NAPK-1", "MPPK-1", "NSPK-1", "MEPK-1", 42)),
            sequences(RowFactory.create("NSPK-1", "NS-1", "GGCCTTAA", "clean")),
            protocols(RowFactory.create("MPPK-1", "COI", "metabarcoding")));

    List<Map<String, String>> legacy = dna(only(OccurrenceCoreBuilder.build(spark, loader).collectAsList()));
    List<Map<String, String>> mapped = dna(only(engine.execute(loader, plan).collectAsList()));

    assertEquals(normalizeLegacy(legacy), mapped);
    assertEquals("GGCCTTAA", mapped.get(0).get(GbifDnaTerm.dna_sequence.qualifiedName()));
    assertEquals("COI", mapped.get(0).get(MixsTerm.target_gene.qualifiedName()));
    assertEquals("42", mapped.get(0).get(TargetTerms.resolve("readCount")));
  }

  @Test
  void multipleAnalysesOnOneMaterialRemainIndependentRows() {
    TableLoader loader =
        loader(
            material("MEPK-1", "OCC001"),
            analyses(
                RowFactory.create("NAPK-1", "MPPK-1", "NSPK-1", "MEPK-1", 10),
                RowFactory.create("NAPK-2", "MPPK-2", "NSPK-2", "MEPK-1", 20)),
            sequences(
                RowFactory.create("NSPK-1", "NS-1", "AAAA", null),
                RowFactory.create("NSPK-2", "NS-2", "CCCC", null)),
            protocols(
                RowFactory.create("MPPK-1", "COI", null),
                RowFactory.create("MPPK-2", "16S", null)));

    List<Map<String, String>> legacy = normalizeLegacy(dna(only(OccurrenceCoreBuilder.build(spark, loader).collectAsList())));
    List<Map<String, String>> mapped = dna(only(engine.execute(loader, plan).collectAsList()));

    Comparator<Map<String, String>> bySequence =
        Comparator.comparing(row -> row.get(GbifDnaTerm.dna_sequence.qualifiedName()));
    assertEquals(2, mapped.size());
    assertEquals(legacy.stream().sorted(bySequence).toList(), mapped.stream().sorted(bySequence).toList());
  }

  @Test
  void ambiguousEvidenceMaterialSuppressesDnaExtension() {
    Dataset<Row> materials =
        spark.createDataFrame(
            List.of(
                RowFactory.create("MEPK-1", "OCC001"),
                RowFactory.create("MEPK-2", "OCC001")),
            new StructType()
                .add("materialEntity_pk", DataTypes.StringType)
                .add("evidenceForOccurrenceID", DataTypes.StringType));
    TableLoader loader =
        loader(
            materials,
            analyses(RowFactory.create("NAPK-1", "MPPK-1", "NSPK-1", "MEPK-1", 42)),
            sequences(RowFactory.create("NSPK-1", "NS-1", "GGCCTTAA", null)),
            protocols(RowFactory.create("MPPK-1", "COI", null)));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertFalse(legacy.getExtensions().containsKey(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA));
    assertFalse(mapped.getExtensions().containsKey(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA));
  }

  @Test
  void governingDnaFragmentsShareAnalysisRowIdentity() {
    CompiledExtension extension =
        engine.compile(plan).extensions().stream()
            .filter(e -> e.rowType().equals(NucleotideMapping.ROW_TYPE_DNA_DERIVED_DATA))
            .findFirst()
            .orElseThrow();

    assertEquals(3, extension.fragments().size());

    var base =
        extension.fragments().stream()
            .filter(f -> f.name().equals("material-nucleotide-analysis-for-occurrence"))
            .findFirst()
            .orElseThrow();
    assertEquals("occurrence_pk", base.scopeKey().column());
    assertEquals("nucleotideAnalysis_pk", base.rowIdentity().orElseThrow().column());
    assertEquals("evidenceForOccurrenceID", base.path().relations().get(0).targetColumn());

    var sequence =
        extension.fragments().stream()
            .filter(f -> f.name().equals("material-nucleotide-sequence-for-occurrence"))
            .findFirst()
            .orElseThrow();
    assertEquals("nucleotideAnalysis_pk", sequence.rowMatch().orElseThrow().column());

    var protocol =
        extension.fragments().stream()
            .filter(f -> f.name().equals("material-molecular-protocol-for-occurrence"))
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
      Dataset<Row> material,
      Dataset<Row> analyses,
      Dataset<Row> sequences,
      Dataset<Row> protocols) {
    return TestTableLoader.of(
        "occurrence", occurrence(),
        "material", material,
        "nucleotide-analysis", analyses,
        "nucleotide-sequence", sequences,
        "molecular-protocol", protocols);
  }

  private Dataset<Row> occurrence() {
    return spark.createDataFrame(
        List.of(RowFactory.create("OPK-1", "OCC001")),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> material(String pk, String occurrenceId) {
    return spark.createDataFrame(
        List.of(RowFactory.create(pk, occurrenceId)),
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> analyses(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("nucleotideAnalysis_pk", DataTypes.StringType)
            .add("molecularProtocol_fk", DataTypes.StringType)
            .add("nucleotideSequence_fk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
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
    String occurrenceId = TargetTerms.resolve("occurrenceID");
    return rows.stream()
        .map(
            row -> {
              Map<String, String> normalized = new LinkedHashMap<>(row);
              normalized.remove(occurrenceId);
              return normalized;
            })
        .toList();
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
