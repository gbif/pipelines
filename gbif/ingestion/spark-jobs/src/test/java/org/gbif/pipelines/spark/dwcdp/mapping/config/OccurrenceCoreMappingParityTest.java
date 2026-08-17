package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.OccurrenceCoreBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledCoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.engine.DwcDpMappingEngine;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TableLoader;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OccurrenceCoreMappingParityTest {

  private SparkSession spark;
  private SchemaGraph graph;
  private DwcDpMappingEngine engine;
  private MappingPlan plan;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OccurrenceCoreMappingParityTest").getOrCreate();
    graph = new DwcDpSchemaLoader().current();
    engine = new DwcDpMappingEngine(graph);
    plan = OccurrenceDwcaMapping.withCurrentCoreEnrichment(graph);
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  @Test
  void directProtocolResolvesToSamplingProtocolAndMatchesLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrences(RowFactory.create("OPK-1", "OCC001", "PPK-1", "Quercus robur")),
            "protocol",
            protocols(RowFactory.create("PPK-1", null, null, "Direct sampling")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    String target = DwcTerm.samplingProtocol.qualifiedName();
    assertEquals(legacy.getCoreTerms().get(target), mapped.getCoreTerms().get(target));
    assertEquals("Direct sampling", mapped.getCoreTerms().get(target));
    assertFalse(mapped.getCoreTerms().containsKey("occurrenceProtocol_fk"));
  }

  @Test
  void absentProtocolTableFallsBackToRawFkAndMatchesLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrences(RowFactory.create("OPK-1", "OCC001", "PROTO-RAW", "Quercus robur")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    String target = DwcTerm.samplingProtocol.qualifiedName();
    assertEquals(legacy.getCoreTerms().get(target), mapped.getCoreTerms().get(target));
    assertEquals("PROTO-RAW", mapped.getCoreTerms().get(target));
    assertFalse(mapped.getCoreTerms().containsKey("occurrenceProtocol_fk"));
  }


  @Test
  void organismAndAcceptedIdentificationMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrencesWithOrganism(
                RowFactory.create(
                    "OPK-1", "OCC001", null, "Occurrence oak", "ORG-1", "Occurrence scope")),
            "organism",
            organisms(RowFactory.create("ORG-1", "Organism oak", "Organism scope", "oak-associate")),
            "identification",
            identifications(
                RowFactory.create("IPK-1", "OPK-1", true, "Plantae", "Accepted family"),
                RowFactory.create("IPK-2", "OPK-1", false, "Historical", "Historical family")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    String organismName = DwcTerm.organismName.qualifiedName();
    String organismScope = DwcTerm.organismScope.qualifiedName();
    String family = DwcTerm.family.qualifiedName();
    String associatedOrganisms = TargetTerms.resolve("associatedOrganisms");

    assertEquals(legacy.getCoreTerms().get(organismName), mapped.getCoreTerms().get(organismName));
    assertEquals("Occurrence oak", mapped.getCoreTerms().get(organismName));
    assertEquals("Occurrence scope", mapped.getCoreTerms().get(organismScope));
    assertEquals("oak-associate", mapped.getCoreTerms().get(associatedOrganisms));
    assertEquals("Accepted family", mapped.getCoreTerms().get(family));
  }

  @Test
  void organismFieldFallsBackOnlyWhenOccurrencePhysicalColumnIsAbsent() {
    StructType occurrenceSchema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("organismID", DataTypes.StringType);
    Dataset<Row> occurrence =
        spark.createDataFrame(
            List.of(RowFactory.create("OPK-1", "OCC001", "ORG-1")), occurrenceSchema);
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrence,
            "organism",
            organisms(RowFactory.create("ORG-1", "Organism oak", "Organism scope", "oak-associate")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    String organismName = DwcTerm.organismName.qualifiedName();
    assertEquals("Organism oak", legacy.getCoreTerms().get(organismName));
    assertEquals(legacy.getCoreTerms().get(organismName), mapped.getCoreTerms().get(organismName));
  }

  @Test
  void multipleAcceptedIdentificationsContributeNothingAndMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrencesWithOrganism(
                RowFactory.create("OPK-1", "OCC001", null, "Occurrence oak", null, null)),
            "identification",
            identifications(
                RowFactory.create("IPK-1", "OPK-1", true, "Plantae", "Family one"),
                RowFactory.create("IPK-2", "OPK-1", true, "Plantae", "Family two")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    String family = DwcTerm.family.qualifiedName();
    assertEquals(legacy.getCoreTerms().get(family), mapped.getCoreTerms().get(family));
    assertFalse(mapped.getCoreTerms().containsKey(family));
  }

  @Test
  void singleEvidenceMaterialAndUsagePolicyMatchLegacyWhileAmbiguousMaterialContributesNothing() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrencesForMaterial(
                RowFactory.create("OPK-1", "OCC001", "Occurrence oak"),
                RowFactory.create("OPK-2", "OCC002", "Occurrence pine")),
            "material",
            materials(
                RowFactory.create("MEPK-1", "OCC001", "UP-1", "NHM", "CAT-1", "Material oak"),
                RowFactory.create("MEPK-2", "OCC002", "UP-2", "MZM", "CAT-2A", "Material pine A"),
                RowFactory.create("MEPK-3", "OCC002", "UP-2", "MZM", "CAT-2B", "Material pine B")),
            "usage-policy",
            usagePolicies(
                RowFactory.create("UP-1", "CC_BY_4_0", "Museum X"),
                RowFactory.create("UP-2", "CC0_1_0", "Museum Y")));

    List<ExtendedRecord> legacy =
        OccurrenceCoreBuilder.build(spark, loader).collectAsList().stream()
            .sorted(java.util.Comparator.comparing(ExtendedRecord::getId))
            .toList();
    List<ExtendedRecord> mapped =
        engine.execute(loader, plan).collectAsList().stream()
            .sorted(java.util.Comparator.comparing(ExtendedRecord::getId))
            .toList();

    assertEquals(2, legacy.size());
    assertEquals(2, mapped.size());
    String institutionCode = DwcTerm.institutionCode.qualifiedName();
    String catalogNumber = DwcTerm.catalogNumber.qualifiedName();
    String scientificName = DwcTerm.scientificName.qualifiedName();
    String license = org.gbif.dwc.terms.DcTerm.license.qualifiedName();
    String rightsHolder = org.gbif.dwc.terms.DcTerm.rightsHolder.qualifiedName();

    assertEquals(legacy.get(0).getCoreTerms().get(institutionCode), mapped.get(0).getCoreTerms().get(institutionCode));
    assertEquals("NHM", mapped.get(0).getCoreTerms().get(institutionCode));
    assertEquals("CAT-1", mapped.get(0).getCoreTerms().get(catalogNumber));
    assertEquals("CC_BY_4_0", mapped.get(0).getCoreTerms().get(license));
    assertEquals("Museum X", mapped.get(0).getCoreTerms().get(rightsHolder));
    assertEquals("Occurrence oak", mapped.get(0).getCoreTerms().get(scientificName));

    assertEquals(legacy.get(1).getCoreTerms().get(institutionCode), mapped.get(1).getCoreTerms().get(institutionCode));
    assertNull(mapped.get(1).getCoreTerms().get(institutionCode));
    assertNull(mapped.get(1).getCoreTerms().get(catalogNumber));
    assertNull(mapped.get(1).getCoreTerms().get(license));
    assertNull(mapped.get(1).getCoreTerms().get(rightsHolder));
    assertEquals("Occurrence pine", mapped.get(1).getCoreTerms().get(scientificName));
  }

  @Test
  void materialProvenanceDirectAndJunctionMatchLegacyAndRespectSingleMaterialGate() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrencesForMaterial(
                RowFactory.create("OPK-1", "OCC001", "Occurrence oak"),
                RowFactory.create("OPK-2", "OCC002", "Occurrence pine")),
            "material",
            materialsForProvenance(
                RowFactory.create("MEPK-1", "OCC001", "PRPK-1"),
                RowFactory.create("MEPK-2", "OCC002", "PRPK-1"),
                RowFactory.create("MEPK-3", "OCC002", "PRPK-2")),
            "material-provenance",
            materialProvenanceLinks(
                RowFactory.create("MEPK-1", "PRPK-1"),
                RowFactory.create("MEPK-1", "PRPK-2")),
            "provenance",
            provenances(
                RowFactory.create("PRPK-1", "A", "Fund", "F-1", "P-A", "Project A"),
                RowFactory.create("PRPK-2", "B", "Fund", "F-2", "P-B", "Project B")));

    List<ExtendedRecord> legacy =
        OccurrenceCoreBuilder.build(spark, loader).collectAsList().stream()
            .sorted(java.util.Comparator.comparing(ExtendedRecord::getId))
            .toList();
    List<ExtendedRecord> mapped =
        engine.execute(loader, plan).collectAsList().stream()
            .sorted(java.util.Comparator.comparing(ExtendedRecord::getId))
            .toList();

    assertEquals(2, legacy.size());
    assertEquals(2, mapped.size());
    String funding = TargetTerms.resolve("fundingAttribution");
    String fundingId = TargetTerms.resolve("fundingAttributionID");
    String projectId = TargetTerms.resolve("projectID");
    String projectTitle = TargetTerms.resolve("projectTitle");

    assertEquals(legacy.get(0).getCoreTerms().get(funding), mapped.get(0).getCoreTerms().get(funding));
    assertEquals("Fund|Fund", mapped.get(0).getCoreTerms().get(funding));
    assertEquals("F-1|F-2", mapped.get(0).getCoreTerms().get(fundingId));
    assertEquals("P-A|P-B", mapped.get(0).getCoreTerms().get(projectId));
    assertEquals("Project A|Project B", mapped.get(0).getCoreTerms().get(projectTitle));

    assertEquals(legacy.get(1).getCoreTerms().get(funding), mapped.get(1).getCoreTerms().get(funding));
    assertNull(mapped.get(1).getCoreTerms().get(funding));
    assertNull(mapped.get(1).getCoreTerms().get(fundingId));
    assertNull(mapped.get(1).getCoreTerms().get(projectId));
    assertNull(mapped.get(1).getCoreTerms().get(projectTitle));
  }


  @Test
  void agentIdsResolveNamesKeepIdsAndMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrencesWithAgents(
                RowFactory.create(
                    "OPK-1", "OCC001", "AGT-1", "AGT-2", null, null)),
            "agent",
            agents(
                RowFactory.create("AGT-1", "Jane Collector"),
                RowFactory.create("AGT-2", "Alex Identifier")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()));
    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.identifiedBy.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.identifiedBy.qualifiedName()));
    assertEquals("Jane Collector", mapped.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()));
    assertEquals("Alex Identifier", mapped.getCoreTerms().get(DwcTerm.identifiedBy.qualifiedName()));
    assertEquals("AGT-1", mapped.getCoreTerms().get(DwcTerm.recordedByID.qualifiedName()));
    assertEquals("AGT-2", mapped.getCoreTerms().get(DwcTerm.identifiedByID.qualifiedName()));
  }

  @Test
  void publisherAgentNamesWinOverResolvedNames() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrencesWithAgents(
                RowFactory.create(
                    "OPK-1",
                    "OCC001",
                    "AGT-1",
                    "AGT-2",
                    "Publisher recorder",
                    "Publisher identifier")),
            "agent",
            agents(
                RowFactory.create("AGT-1", "Jane Collector"),
                RowFactory.create("AGT-2", "Alex Identifier")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()));
    assertEquals(
        legacy.getCoreTerms().get(DwcTerm.identifiedBy.qualifiedName()),
        mapped.getCoreTerms().get(DwcTerm.identifiedBy.qualifiedName()));
    assertEquals("Publisher recorder", mapped.getCoreTerms().get(DwcTerm.recordedBy.qualifiedName()));
    assertEquals("Publisher identifier", mapped.getCoreTerms().get(DwcTerm.identifiedBy.qualifiedName()));
  }

  @Test
  void governingMaterialProvenanceFragmentsUseContributionIdentityAndSchemaPaths() {
    List<CompiledCoreFragment> fragments = engine.compile(plan).coreFragments();

    CompiledCoreFragment direct =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-core-material-direct-provenance"))
            .findFirst()
            .orElseThrow();
    CompiledCoreFragment junction =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-core-material-provenance"))
            .findFirst()
            .orElseThrow();

    assertEquals("provenance", direct.path().currentResource());
    assertEquals(2, direct.path().relations().size());
    assertEquals("evidenceForOccurrenceID", direct.path().relations().get(0).targetColumn());
    assertEquals("provenance_fk", direct.path().relations().get(1).sourceColumn());

    assertEquals("provenance", junction.path().currentResource());
    assertEquals(3, junction.path().relations().size());
    assertEquals("materialEntity_fk", junction.path().relations().get(1).targetColumn());
    assertEquals("provenance_fk", junction.path().relations().get(2).sourceColumn());

    assertEquals(5, engine.compile(plan).coreTargetMerges().size());
  }

  @Test
  void materialGeologicalContextMatchesLegacyAndRequiresExactlyOneContext() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrencesForMaterial(
                RowFactory.create("OPK-1", "OCC001", "Occurrence oak"),
                RowFactory.create("OPK-2", "OCC002", "Occurrence pine")),
            "material",
            materialsForContext(
                RowFactory.create("MEPK-1", "OCC001"),
                RowFactory.create("MEPK-2", "OCC002")),
            "material-geological-context",
            materialGeologicalContexts(
                RowFactory.create("MEPK-1", "GCPK-1"),
                RowFactory.create("MEPK-2", "GCPK-1"),
                RowFactory.create("MEPK-2", "GCPK-2")),
            "geological-context",
            geologicalContexts(
                RowFactory.create("GCPK-1", "GEO-1", "Morrison", "Brushy Basin"),
                RowFactory.create("GCPK-2", "GEO-2", "Chinle", "Petrified Forest")));

    List<ExtendedRecord> legacy =
        OccurrenceCoreBuilder.build(spark, loader).collectAsList().stream()
            .sorted(java.util.Comparator.comparing(ExtendedRecord::getId))
            .toList();
    List<ExtendedRecord> mapped =
        engine.execute(loader, plan).collectAsList().stream()
            .sorted(java.util.Comparator.comparing(ExtendedRecord::getId))
            .toList();

    String formation = DwcTerm.formation.qualifiedName();
    String bed = DwcTerm.bed.qualifiedName();
    assertEquals(legacy.get(0).getCoreTerms().get(formation), mapped.get(0).getCoreTerms().get(formation));
    assertEquals("Morrison", mapped.get(0).getCoreTerms().get(formation));
    assertEquals("Brushy Basin", mapped.get(0).getCoreTerms().get(bed));
    assertEquals(legacy.get(1).getCoreTerms().get(formation), mapped.get(1).getCoreTerms().get(formation));
    assertNull(mapped.get(1).getCoreTerms().get(formation));
    assertNull(mapped.get(1).getCoreTerms().get(bed));
  }

  @Test
  void materialProtocolsMergeWithDirectSamplingProtocolAndMatchLegacy() {
    TableLoader loader =
        TestTableLoader.of(
            "occurrence",
            occurrences(RowFactory.create("OPK-1", "OCC001", "PPK-D", "Occurrence oak")),
            "material",
            materialsForContext(RowFactory.create("MEPK-1", "OCC001")),
            "material-protocol",
            materialProtocols(
                RowFactory.create("MEPK-1", "PPK-B"),
                RowFactory.create("MEPK-1", "PPK-A")),
            "protocol",
            protocols(
                RowFactory.create("PPK-D", null, null, "Direct sampling"),
                RowFactory.create("PPK-A", null, null, "Material A"),
                RowFactory.create("PPK-B", null, null, "Material B")));

    ExtendedRecord legacy = only(OccurrenceCoreBuilder.build(spark, loader).collectAsList());
    ExtendedRecord mapped = only(engine.execute(loader, plan).collectAsList());

    String target = DwcTerm.samplingProtocol.qualifiedName();
    assertEquals(legacy.getCoreTerms().get(target), mapped.getCoreTerms().get(target));
    assertEquals("Direct sampling|Material A|Material B", mapped.getCoreTerms().get(target));
  }

  @Test
  void governingMaterialContextAndProtocolFragmentsUseSingleMaterialGate() {
    List<CompiledCoreFragment> fragments = engine.compile(plan).coreFragments();

    CompiledCoreFragment context =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-core-material-geological-context"))
            .findFirst()
            .orElseThrow();
    assertEquals("geological-context", context.path().currentResource());
    assertEquals(3, context.path().relations().size());
    assertEquals("evidenceForOccurrenceID", context.path().relations().get(0).targetColumn());
    assertEquals("materialEntity_fk", context.path().relations().get(1).targetColumn());
    assertEquals("geologicalContext_fk", context.path().relations().get(2).sourceColumn());

    CompiledCoreFragment protocols =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-core-material-protocols"))
            .findFirst()
            .orElseThrow();
    assertEquals("protocol", protocols.path().currentResource());
    assertEquals(3, protocols.path().relations().size());
    assertEquals("materialEntity_fk", protocols.path().relations().get(1).targetColumn());
    assertEquals("protocol_fk", protocols.path().relations().get(2).sourceColumn());
    assertEquals(DwcTerm.samplingProtocol.qualifiedName(), protocols.targets().get(0).targetTerm());
  }

  @Test
  void governingMaterialFragmentUsesWeakEvidenceRelationThenUsagePolicy() {
    CompiledCoreFragment material =
        engine.compile(plan).coreFragments().stream()
            .filter(f -> f.name().equals("occurrence-core-material"))
            .findFirst()
            .orElseThrow();

    assertEquals("usage-policy", material.path().currentResource());
    assertEquals(2, material.path().relations().size());
    assertEquals("evidenceForOccurrenceID", material.path().relations().get(0).targetColumn());
    assertEquals("usagePolicy_fk", material.path().relations().get(1).sourceColumn());
  }

  @Test
  void governingCoreFragmentsUseSchemaBackedRowRelations() {
    List<CompiledCoreFragment> fragments = engine.compile(plan).coreFragments();

    CompiledCoreFragment organism =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-core-organism"))
            .findFirst()
            .orElseThrow();
    assertEquals("organism", organism.path().currentResource());
    assertEquals("organismID", organism.path().relations().get(0).sourceColumn());

    CompiledCoreFragment identification =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-core-accepted-identification"))
            .findFirst()
            .orElseThrow();
    assertEquals("identification", identification.path().currentResource());
    assertEquals("occurrence_pk", identification.path().relations().get(0).sourceColumn());
    assertEquals("occurrence_fk", identification.path().relations().get(0).targetColumn());
  }


  @Test
  void governingAgentFragmentsUseDeclaredMappingRelationsAndOneOfTargets() {
    List<CompiledCoreFragment> fragments = engine.compile(plan).coreFragments();

    CompiledCoreFragment recordedBy =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-recorded-by-agent"))
            .findFirst()
            .orElseThrow();
    CompiledCoreFragment identifiedBy =
        fragments.stream()
            .filter(f -> f.name().equals("occurrence-identified-by-agent"))
            .findFirst()
            .orElseThrow();

    assertEquals("recordedByID", recordedBy.path().relations().get(0).sourceColumn());
    assertEquals("agentID", recordedBy.path().relations().get(0).targetColumn());
    assertEquals(DwcTerm.recordedBy.qualifiedName(), recordedBy.targets().get(0).targetTerm());

    assertEquals("identifiedByID", identifiedBy.path().relations().get(0).sourceColumn());
    assertEquals("agentID", identifiedBy.path().relations().get(0).targetColumn());
    assertEquals(DwcTerm.identifiedBy.qualifiedName(), identifiedBy.targets().get(0).targetTerm());
  }

  @Test
  void governingProtocolFragmentUsesSchemaBackedOccurrenceFk() {
    CompiledCoreFragment fragment =
        engine.compile(plan).coreFragments().stream()
            .filter(f -> f.name().equals("occurrence-direct-sampling-protocol"))
            .findFirst()
            .orElseThrow();

    assertEquals("protocol", fragment.path().currentResource());
    assertEquals(1, fragment.path().relations().size());
    assertEquals("occurrenceProtocol_fk", fragment.path().relations().get(0).sourceColumn());
  }

  private Dataset<Row> occurrences(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("occurrenceProtocol_fk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }



  private Dataset<Row> occurrencesWithAgents(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("recordedByID", DataTypes.StringType)
            .add("identifiedByID", DataTypes.StringType)
            .add("recordedBy", DataTypes.StringType)
            .add("identifiedBy", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> agents(Row... rows) {
    StructType schema =
        new StructType()
            .add("agentID", DataTypes.StringType)
            .add("preferredAgentName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> occurrencesWithOrganism(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("occurrenceProtocol_fk", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("organismScope", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> organisms(Row... rows) {
    StructType schema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("organismScope", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> identifications(Row... rows) {
    StructType schema =
        new StructType()
            .add("identification_pk", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("isAcceptedIdentification", DataTypes.BooleanType)
            .add("kingdom", DataTypes.StringType)
            .add("family", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> occurrencesForMaterial(Row... rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materials(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("usagePolicy_fk", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> usagePolicies(Row... rows) {
    StructType schema =
        new StructType()
            .add("usagePolicy_pk", DataTypes.StringType)
            .add("license", DataTypes.StringType)
            .add("rightsHolder", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materialsForProvenance(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materialProvenanceLinks(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("provenance_fk", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> provenances(Row... rows) {
    StructType schema =
        new StructType()
            .add("provenance_pk", DataTypes.StringType)
            .add("provenanceID", DataTypes.StringType)
            .add("fundingAttribution", DataTypes.StringType)
            .add("fundingAttributionID", DataTypes.StringType)
            .add("projectID", DataTypes.StringType)
            .add("projectTitle", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materialsForContext(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materialGeologicalContexts(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("geologicalContext_fk", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> geologicalContexts(Row... rows) {
    StructType schema =
        new StructType()
            .add("geologicalContext_pk", DataTypes.StringType)
            .add("geologicalContextID", DataTypes.StringType)
            .add("formation", DataTypes.StringType)
            .add("bed", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> materialProtocols(Row... rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_fk", DataTypes.StringType)
            .add("protocol_fk", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private Dataset<Row> protocols(Row... rows) {
    StructType schema =
        new StructType()
            .add("protocol_pk", DataTypes.StringType)
            .add("protocolType", DataTypes.StringType)
            .add("protocolName", DataTypes.StringType)
            .add("protocolDescription", DataTypes.StringType);
    return spark.createDataFrame(List.of(rows), schema);
  }

  private static ExtendedRecord only(List<ExtendedRecord> records) {
    assertEquals(1, records.size());
    return records.get(0);
  }
}
