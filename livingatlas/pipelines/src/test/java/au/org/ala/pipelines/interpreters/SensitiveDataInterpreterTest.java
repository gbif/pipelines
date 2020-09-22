package au.org.ala.pipelines.interpreters;

import static org.junit.Assert.*;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import au.org.ala.sds.api.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SensitiveDataInterpreterTest {
  private static final String DATARESOURCE_UID = "drTest";

  private ALACollectoryMetadata dataResource;
  private Map<SpeciesCheck, Boolean> sensitivityMap;
  private KeyValueStore<SpeciesCheck, Boolean> sensitivityLookup;
  private ConservationApi conservationApi;
  private Set<String> sensitiveFields;

  @Before
  public void setUp() throws Exception {
    Map<String, String> defaults = new HashMap<>();
    defaults.put("kingdom", "Plantae");
    List<Map<String, String>> hints = new ArrayList<>();
    hints.add(Collections.singletonMap("phylum", "Charophyta"));
    hints.add(Collections.singletonMap("phylum", "Bryophyta"));

    this.dataResource =
        ALACollectoryMetadata.builder()
            .name("Test data resource")
            .uid(DATARESOURCE_UID)
            .defaultDarwinCoreValues(defaults)
            .taxonomyCoverageHints(hints)
            .build();
    this.sensitiveFields =
        Arrays.asList(
                DwcTerm.scientificName,
                DwcTerm.taxonConceptID,
                DwcTerm.eventDate,
                DwcTerm.decimalLatitude,
                DwcTerm.decimalLongitude,
                DwcTerm.locality,
                DwcTerm.municipality,
                DwcTerm.verbatimCoordinates,
                DwcTerm.verbatimLatitude,
                DwcTerm.verbatimLongitude)
            .stream()
            .map(DwcTerm::simpleName)
            .collect(Collectors.toSet());
    this.sensitivityLookup =
        new KeyValueStore<SpeciesCheck, Boolean>() {
          @Override
          public Boolean get(SpeciesCheck speciesCheck) {
            return sensitivityMap.get(speciesCheck);
          }

          @Override
          public void close() throws IOException {}
        };
    this.sensitivityMap = new HashMap<>();
    SpeciesCheck search =
        SpeciesCheck.builder()
            .scientificName("Acacia dealbata")
            .taxonId("https://id.biodiversity.org.au/taxon/apni/51286863")
            .build();
    this.sensitivityMap.put(search, true);
    search = SpeciesCheck.builder().scientificName("Acacia dealbata").build();
    this.sensitivityMap.put(search, true);
    this.conservationApi =
        new ConservationApi() {
          @Override
          public Set<String> getSensitiveDataFields() {
            return SensitiveDataInterpreterTest.this.sensitiveFields;
          }

          @Override
          public boolean isSensitive(SpeciesCheck speciesCheck) {
            return SensitiveDataInterpreterTest.this.sensitivityMap.getOrDefault(
                speciesCheck, false);
          }

          @Override
          public boolean isSensitive(String s, String s1) {
            return this.isSensitive(SpeciesCheck.builder().scientificName(s).taxonId(s1).build());
          }

          @Override
          public SensitivityReport process(SensitivityQuery sensitivityQuery) {
            boolean sensitive =
                this.isSensitive(
                    sensitivityQuery.getScientificName(), sensitivityQuery.getTaxonId());
            SensitivityReport.SensitivityReportBuilder builder = SensitivityReport.builder();
            builder.sensitive(sensitive);
            builder.accessControl(false);
            builder.valid(true);
            builder.loadable(true);
            if (sensitive) {
              String decimalLatitude = DwcTerm.decimalLatitude.simpleName();
              String decimalLongitude = DwcTerm.decimalLongitude.simpleName();
              Map<String, String> original = new HashMap<>();
              original.put(decimalLatitude, sensitivityQuery.getProperties().get(decimalLatitude));
              original.put(
                  decimalLongitude, sensitivityQuery.getProperties().get(decimalLongitude));
              Map<String, Object> results = new HashMap<>();
              results.put(SensitiveDataInterpreter.DATA_GENERALIZATIONS, "Test generalisation");
              results.put(SensitiveDataInterpreter.GENERALISATION_TO_APPLY_IN_METRES, "10000");
              results.put(SensitiveDataInterpreter.GENERALISATION_IN_METRES, "10000");
              results.put(SensitiveDataInterpreter.ORIGINAL_VALUES, original);
              results.put(decimalLatitude, "-32.0");
              results.put(decimalLongitude, "150.0");
              builder.result(results);
              builder.report(ValidationReport.builder().category("Sensitive").build());
            }
            return builder.build();
          }
        };
  }

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testConstructFields1() throws Exception {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.simpleName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.simpleName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.simpleName(), "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.simpleName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.simpleName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, er);
    assertFalse(properties.isEmpty());
    for (String field : this.sensitiveFields) {
      String val = map.get(field);
      assertEquals("Fields " + field + " don't match", val, properties.get(field));
    }
    for (String field : map.keySet()) {
      if (!this.sensitiveFields.contains(field))
        assertNull("Fields " + field + " should be absent", properties.get(field));
    }
  }

  @Test
  public void testConstructFields2() throws Exception {
    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId("1")
            .setCountry("Australia")
            .setDecimalLatitude(-39.78)
            .setDecimalLongitude(149.55)
            .setLocality("Somewhere")
            .setDepth(10.0)
            .build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, lr);
    assertFalse(properties.isEmpty());
    for (String field : this.sensitiveFields) {
      if (lr.getSchema().getField(field) != null) {
        Object val = lr.get(field);
        assertEquals(
            "Fields " + field + " don't match",
            val == null ? null : val.toString(),
            properties.get(field));
      }
    }
    for (Schema.Field field : lr.getSchema().getFields()) {
      if (!this.sensitiveFields.contains(field.name()))
        assertNull("Fields " + field + " should be absent", properties.get(field.name()));
    }
  }

  @Test
  public void testConstructFields3() throws Exception {
    Nomenclature nomenclature =
        Nomenclature.newBuilder()
            .setId("ICZN")
            .setSource("International Code for Zoologcal Names")
            .build();
    TaxonRecord tr =
        TaxonRecord.newBuilder()
            .setId("1")
            .setNomenclature(nomenclature)
            .setAcceptedUsage(
                RankedName.newBuilder()
                    .setKey(26)
                    .setRank(Rank.SPECIES)
                    .setName("Acacia dealbata")
                    .build())
            .build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, tr);
    assertFalse(properties.isEmpty());
    assertEquals(2, properties.size());
    assertEquals("Acacia dealbata", properties.get(DwcTerm.scientificName.simpleName()));
    assertEquals("26", properties.get(DwcTerm.taxonConceptID.simpleName()));
  }

  @Test
  public void testConstructFields4() throws Exception {
    String date = "2020-03-01";
    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId("1")
            .setEventDate(EventDate.newBuilder().setGte("2020-03-01").setLte("2020-03-02").build())
            .setDay(1)
            .setMonth(3)
            .setYear(2020)
            .build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, tr);
    assertFalse(properties.isEmpty());
    assertEquals(1, properties.size());
    assertEquals("2020-03-01", properties.get(DwcTerm.eventDate.simpleName()));
  }

  @Test
  public void testConstructFields5() throws Exception {
    ALATaxonRecord tr =
        ALATaxonRecord.newBuilder()
            .setId("1")
            .setScientificName("Acacia dealbata")
            .setTaxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .setKingdom("Plantae")
            .setScientificNameAuthorship("Link")
            .setRank("species")
            .build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, tr);
    assertFalse(properties.isEmpty());
    for (String field : this.sensitiveFields) {
      if (tr.getSchema().getField(field) != null) {
        Object val = tr.get(field);
        assertEquals(
            "Fields " + field + " don't match",
            val == null ? null : val.toString(),
            properties.get(field));
      }
    }
    for (Schema.Field field : tr.getSchema().getFields()) {
      if (!this.sensitiveFields.contains(field.name()))
        assertNull("Fields " + field + " should be absent", properties.get(field.name()));
    }
  }

  @Test
  public void testApplySensitivity1() throws Exception {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.simpleName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.simpleName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.simpleName(), "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.simpleName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.simpleName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.decimalLatitude.simpleName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.simpleName(), "149.6");
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(sr, er);
    assertEquals("Acacia dealbata", er.getCoreTerms().get(DwcTerm.scientificName.simpleName()));
    assertEquals("2020-01-01", er.getCoreTerms().get(DwcTerm.eventDate.simpleName()));
    assertEquals("-39.8", er.getCoreTerms().get(DwcTerm.decimalLatitude.simpleName()));
    assertEquals("149.6", er.getCoreTerms().get(DwcTerm.decimalLongitude.simpleName()));
  }

  @Test
  public void testApplySensitivity2() throws Exception {
    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId("1")
            .setCountry("Australia")
            .setDecimalLatitude(-39.78)
            .setDecimalLongitude(149.55)
            .setLocality("Somewhere")
            .setDepth(10.0)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.decimalLatitude.simpleName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.simpleName(), "149.6");
    properties.put(DwcTerm.locality.simpleName(), null);
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(sr, lr);
    assertEquals(-39.8, lr.getDecimalLatitude(), 0.0001);
    assertEquals(149.6, lr.getDecimalLongitude(), 0.0001);
    assertNull(lr.getLocality());
  }

  @Test
  public void testApplySensitivity3() throws Exception {
    Nomenclature nomenclature =
        Nomenclature.newBuilder()
            .setId("ICZN")
            .setSource("International Code for Zoologcal Names")
            .build();
    TaxonRecord tr =
        TaxonRecord.newBuilder()
            .setId("1")
            .setNomenclature(nomenclature)
            .setAcceptedUsage(
                RankedName.newBuilder()
                    .setKey(26)
                    .setRank(Rank.SPECIES)
                    .setName("Acacia dealbata")
                    .build())
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.scientificName.simpleName(), "Acacia");
    properties.put(DwcTerm.decimalLatitude.simpleName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.simpleName(), "149.6");
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(sr, tr);
    assertEquals("Acacia", tr.getAcceptedUsage().getName());
  }

  @Test
  public void testApplySensitivity4() throws Exception {
    String date = "2020-03-01";
    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId("1")
            .setEventDate(EventDate.newBuilder().setGte("2020-03-01").setLte("2020-03-02").build())
            .setDay(1)
            .setMonth(3)
            .setYear(2020)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.scientificName.simpleName(), "Acacia");
    properties.put(DwcTerm.eventDate.simpleName(), "2020");
    properties.put(DwcTerm.year.simpleName(), "2020");
    properties.put(DwcTerm.month.simpleName(), null);
    properties.put(DwcTerm.day.simpleName(), null);
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(sr, tr);
    assertEquals("2020", tr.getEventDate().getGte());
    assertEquals("2020", tr.getEventDate().getLte());
    assertEquals(2020, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
  }

  @Test
  public void testApplySensitivity5() throws Exception {
    ALATaxonRecord tr =
        ALATaxonRecord.newBuilder()
            .setId("1")
            .setScientificName("Acacia dealbata")
            .setTaxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .setKingdom("Plantae")
            .setScientificNameAuthorship("Link")
            .setRank("species")
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.scientificName.simpleName(), "Acacia");
    properties.put(DwcTerm.decimalLatitude.simpleName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.simpleName(), "149.6");
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(sr, tr);
    assertEquals("Acacia", tr.getScientificName());
    assertEquals("species", tr.getRank());
  }

  @Test
  public void testSourceQualityChecks1() throws Exception {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.simpleName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.simpleName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.simpleName(), "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.simpleName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.simpleName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, er);
    SensitiveDataInterpreter.sourceQualityChecks(properties, sr, this.dataResource);
    assertTrue(sr.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void testSourceQualityChecks2() throws Exception {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.simpleName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.simpleName(), "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.simpleName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.simpleName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, er);
    SensitiveDataInterpreter.sourceQualityChecks(properties, sr, this.dataResource);
    assertFalse(sr.getIssues().getIssueList().isEmpty());
    assertTrue(
        sr.getIssues().getIssueList().contains(ALAOccurrenceIssue.NAME_NOT_SUPPLIED.getId()));
  }

  @Test
  public void testSensitiveDataInterpreter1() throws Exception {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.simpleName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.simpleName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.simpleName(), "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.simpleName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.simpleName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitiveFields, properties, er);
    SensitiveDataInterpreter.sensitiveDataInterpreter(
        dataResource, this.sensitivityLookup, this.conservationApi, properties, sr);
    assertTrue(sr.getIssues().getIssueList().isEmpty());
    assertTrue(sr.getSensitive());
    assertEquals("Test generalisation", sr.getDataGeneralizations());
    assertEquals("10000", sr.getGeneralisationInMetres());
    assertEquals("10000", sr.getGeneralisationToApplyInMetres());
    assertEquals(2, sr.getAltered().size());
    assertEquals("-32.0", sr.getAltered().get(DwcTerm.decimalLatitude.simpleName()));
    assertEquals("150.0", sr.getAltered().get(DwcTerm.decimalLongitude.simpleName()));
    assertEquals(2, sr.getOriginal().size());
    assertEquals("-39.78", sr.getOriginal().get(DwcTerm.decimalLatitude.simpleName()));
    assertEquals("149.55", sr.getOriginal().get(DwcTerm.decimalLongitude.simpleName()));
  }
}
