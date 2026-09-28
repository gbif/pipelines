package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkExtendedRecordExecutor;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BasisOfRecordOccurrenceMappingTest {

  private SparkSession spark;
  private SchemaGraph graph;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName(getClass().getSimpleName()).getOrCreate();
    graph = new DwcDpSchemaLoader().current();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void canonicalOccurrenceMappingDerivesBasisOfRecordWithMaterialPrecedence() {
    List<ExtendedRecord> records =
        new SparkExtendedRecordExecutor(graph)
            .execute(
                TestTableLoader.of(
                    "occurrence", occurrences(), "event", events(), "material", materials()),
                OccurrenceDwcaMapping.current(graph))
            .collectAsList();

    Map<String, String> actual =
        records.stream()
            .collect(
                Collectors.toMap(
                    ExtendedRecord::getId,
                    record -> record.getCoreTerms().get(DwcTerm.basisOfRecord.qualifiedName())));

    assertEquals("PreservedSpecimen", actual.get("occ-preserved"));
    assertEquals("FossilSpecimen", actual.get("occ-fossil"));
    assertEquals("LivingSpecimen", actual.get("occ-living"));
    assertEquals("MaterialSample", actual.get("occ-tissue"));
    assertEquals("MaterialSample", actual.get("occ-dna"));
    assertEquals("MaterialSample", actual.get("occ-nucleotide"));
    assertEquals("MachineObservation", actual.get("occ-sensor"));
    assertEquals("HumanObservation", actual.get("occ-observation"));
    assertEquals("Occurrence", actual.get("occ-other"));
    assertEquals("Occurrence", actual.get("occ-no-event"));
  }

  private Dataset<Row> occurrences() {
    return spark.createDataFrame(
        List.of(
            occurrence("O1", "occ-preserved", "E1"),
            occurrence("O2", "occ-fossil", "E2"),
            occurrence("O3", "occ-living", "E3"),
            occurrence("O4", "occ-tissue", "E4"),
            occurrence("O5", "occ-dna", "E5"),
            occurrence("O6", "occ-nucleotide", "E6"),
            occurrence("O7", "occ-sensor", "E7"),
            occurrence("O8", "occ-observation", "E8"),
            occurrence("O9", "occ-other", "E9"),
            occurrence("O10", "occ-no-event", null)),
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType));
  }

  private Dataset<Row> events() {
    return spark.createDataFrame(
        List.of(
            event("E1", "Observation"),
            event("E2", "Sensor"),
            event("E3", "Sensor"),
            event("E4", "Sensor"),
            event("E5", "Observation"),
            event("E6", "NucleotideAnalysis"),
            event("E7", "Sensor"),
            event("E8", "Observation"),
            event("E9", "Other")),
        new StructType()
            .add("event_pk", DataTypes.StringType)
            .add("eventType", DataTypes.StringType));
  }

  private Dataset<Row> materials() {
    List<Row> rows = new ArrayList<>();
    rows.add(material("M1", "occ-preserved", "preserved"));
    rows.add(material("M2", "occ-fossil", "fossilized"));
    rows.add(material("M3", "occ-living", "living"));
    rows.add(material("M4", "occ-tissue", "tissue"));
    rows.add(material("M5", "occ-dna", "DNA extract"));
    rows.add(material("M6", "occ-nucleotide", null));
    rows.add(material("M7", "occ-sensor", null));
    rows.add(material("M8", "occ-observation", null));
    rows.add(material("M9", "occ-other", null));
    return spark.createDataFrame(
        rows,
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("materialEntityCategory", DataTypes.StringType));
  }

  private static Row occurrence(String pk, String id, String eventFk) {
    return RowFactory.create(pk, id, eventFk);
  }

  private static Row event(String pk, String eventType) {
    return RowFactory.create(pk, eventType);
  }

  private static Row material(String pk, String occurrenceId, String category) {
    return RowFactory.create(pk, occurrenceId, category);
  }
}
