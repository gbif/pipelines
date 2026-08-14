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
import org.gbif.dwc.terms.DcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.TermResolver;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.UsagePolicyJoinBuilder;
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
class OccurrenceMultimediaMappingParityTest {

  private SparkSession spark;
  private SchemaGraph graph;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OccurrenceMultimediaMappingParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = OccurrenceDwcaMapping.withMultimedia(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void directOccurrenceMediaMatchesLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-1", null, "https://example.com/occurrence.jpg")),
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
            occurrenceMedia(RowFactory.create("OPK-1", "MPK-1")));

    assertPayloadParity(loader, 1);
  }

  @Test
  void directOccurrenceMediaCarriesUsagePolicy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-1", "UP-1", "https://example.com/licensed.jpg")),
            UsagePolicyJoinBuilder.TABLE_USAGE_POLICY,
            usagePolicy(RowFactory.create("UP-1", "CC_BY_4_0", "Museum X")),
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
            occurrenceMedia(RowFactory.create("OPK-1", "MPK-1")));

    List<Map<String, String>> mapped = mappedMedia(loader);
    assertPayloadParity(loader, 1);
    assertEquals("CC_BY_4_0", mapped.get(0).get(DcTerm.license.qualifiedName()));
    assertEquals("Museum X", mapped.get(0).get(DcTerm.rightsHolder.qualifiedName()));
  }

  @Test
  void materialMediaMatchesLegacyForExactlyOneEvidenceMaterial() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-1", null, "https://example.com/material.jpg")),
            MediaExtensionBuilder.TABLE_MATERIAL_MEDIA,
            materialMedia(RowFactory.create("MEPK-1", "MPK-1")));

    assertPayloadParity(loader, 1);
  }

  @Test
  void ambiguousEvidenceMaterialSuppressesMaterialMedia() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(
                RowFactory.create("MEPK-1", "OCC001"),
                RowFactory.create("MEPK-2", "OCC001")),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(RowFactory.create("MPK-1", null, "https://example.com/ambiguous.jpg")),
            MediaExtensionBuilder.TABLE_MATERIAL_MEDIA,
            materialMedia(RowFactory.create("MEPK-1", "MPK-1")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());
    assertFalse(legacy.getExtensions().containsKey(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA));
    assertFalse(mapped.getExtensions().containsKey(MultimediaMapping.ROW_TYPE_MULTIMEDIA));
  }

  @Test
  void directAndMaterialMediaAreUnionedWithoutCartesianProduct() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence", occurrence(),
            MaterialJoinBuilder.TABLE_MATERIAL,
            material(RowFactory.create("MEPK-1", "OCC001")),
            MediaExtensionBuilder.TABLE_MEDIA,
            media(
                RowFactory.create("MPK-1", null, "https://example.com/direct.jpg"),
                RowFactory.create("MPK-2", null, "https://example.com/material.jpg")),
            MediaExtensionBuilder.TABLE_OCCURRENCE_MEDIA,
            occurrenceMedia(RowFactory.create("OPK-1", "MPK-1")),
            MediaExtensionBuilder.TABLE_MATERIAL_MEDIA,
            materialMedia(RowFactory.create("MEPK-1", "MPK-2")));

    assertPayloadParity(loader, 2);
  }

  @Test
  void governingMultimediaFragmentsUseOccurrenceOwnershipAndMaterialGate() {
    CompiledExtension extension =
        engine.compile(plan).extensions().stream()
            .filter(e -> e.rowType().equals(MultimediaMapping.ROW_TYPE_MULTIMEDIA))
            .findFirst()
            .orElseThrow();

    assertEquals(2, extension.fragments().size());
    assertEquals(50, extension.maxRowsPerParent().orElseThrow());

    var direct =
        extension.fragments().stream().filter(f -> f.name().equals("occurrence-media")).findFirst().orElseThrow();
    assertEquals("occurrence_fk", direct.scopeKey().column());
    assertEquals("media", direct.path().relations().get(0).targetResource());

    var material =
        extension.fragments().stream()
            .filter(f -> f.name().equals("material-media-for-occurrence"))
            .findFirst()
            .orElseThrow();
    assertEquals("occurrence_pk", material.scopeKey().column());
    assertEquals("evidenceForOccurrenceID", material.path().relations().get(0).targetColumn());
  }

  private void assertPayloadParity(TableLoader loader, int expectedRows) {
    List<Map<String, String>> legacy = normalizeLegacy(legacyMedia(loader));
    List<Map<String, String>> mapped = mappedMedia(loader);
    assertEquals(expectedRows, legacy.size());
    assertEquals(expectedRows, mapped.size());

    String accessUri = TermResolver.resolve("accessURI");
    Comparator<Map<String, String>> byAccessUri =
        Comparator.comparing(row -> row.get(accessUri), Comparator.nullsFirst(String::compareTo));
    assertEquals(
        legacy.stream().sorted(byAccessUri).toList(),
        mapped.stream().sorted(byAccessUri).toList());
  }

  private List<Map<String, String>> legacyMedia(TableLoader loader) {
    ExtendedRecord record = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    List<Map<String, String>> rows = record.getExtensions().get(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> mappedMedia(TableLoader loader) {
    ExtendedRecord record = only(engine.execute(loader, plan).collectAsList());
    List<Map<String, String>> rows = record.getExtensions().get(MultimediaMapping.ROW_TYPE_MULTIMEDIA);
    assertNotNull(rows);
    return rows;
  }

  private List<Map<String, String>> normalizeLegacy(List<Map<String, String>> rows) {
    String occurrenceId = TermResolver.resolve("occurrenceID");
    return rows.stream()
        .map(
            row -> {
              Map<String, String> normalized = new LinkedHashMap<>(row);
              normalized.remove(occurrenceId);
              return normalized;
            })
        .toList();
  }

  private Dataset<Row> occurrence() {
    return spark.createDataFrame(
        List.of(RowFactory.create("OPK-1", "OCC001")),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> media(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("media_pk", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("accessURI", DataTypes.StringType));
  }

  private Dataset<Row> occurrenceMedia(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("occurrence_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType));
  }

  private Dataset<Row> material(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType));
  }

  private Dataset<Row> materialMedia(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("media_fk", DataTypes.StringType));
  }

  private Dataset<Row> usagePolicy(Row... rows) {
    return spark.createDataFrame(
        List.of(rows),
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType));
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
