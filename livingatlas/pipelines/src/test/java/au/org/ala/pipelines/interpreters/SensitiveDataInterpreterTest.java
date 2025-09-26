package au.org.ala.pipelines.interpreters;

import static org.junit.Assert.*;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.transforms.IndexFields;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import au.org.ala.pipelines.vocabulary.Sensitivity;
import au.org.ala.pipelines.vocabulary.Vocab;
import au.org.ala.sds.api.*;
import au.org.ala.sds.generalise.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.*;
import org.junit.Before;
import org.junit.Test;

public class SensitiveDataInterpreterTest {
  private static final String DATARESOURCE_UID = "drTest";

  private ALACollectoryMetadata dataResource;
  private Map<SpeciesCheck, Boolean> sensitivityMap;
  private KeyValueStore<SpeciesCheck, Boolean> sensitivityLookup;
  private KeyValueStore<SensitivityQuery, SensitivityReport> sensitivityReportLookup;
  private ConservationApi conservationApi;
  private List<Generalisation> generalisations;
  private Set<Term> sensitive;
  private Vocab sensitiveVocab;

  @Before
  public void setUp() throws Exception {
    Locale.setDefault(Locale.US);
    this.dataResource =
        ALACollectoryMetadata.builder().name("Test data resource").uid(DATARESOURCE_UID).build();
    this.generalisations =
        Arrays.asList(
            new RetainGeneralisation(DwcTerm.scientificName),
            new RetainGeneralisation(DwcTerm.taxonConceptID),
            new RetainGeneralisation(DwcTerm.stateProvince),
            new ClearGeneralisation(DwcTerm.eventDate),
            new ClearGeneralisation(SensitiveDataInterpreter.EVENT_DATE_END_TERM),
            new LatLongGeneralisation(DwcTerm.decimalLatitude, DwcTerm.decimalLongitude),
            new ClearGeneralisation(DwcTerm.locality),
            new RetainGeneralisation(DwcTerm.municipality),
            new ClearGeneralisation(DwcTerm.verbatimCoordinates),
            new ClearGeneralisation(DwcTerm.verbatimLatitude),
            new ClearGeneralisation(DwcTerm.verbatimLongitude),
            new MessageGeneralisation(
                DwcTerm.dataGeneralizations,
                "Test generalisation",
                true,
                MessageGeneralisation.Trigger.ANY),
            new AddingGeneralisation(DwcTerm.coordinateUncertaintyInMeters, true, true, 0));
    this.sensitivityLookup =
        new KeyValueStore<SpeciesCheck, Boolean>() {
          @Override
          public Boolean get(SpeciesCheck speciesCheck) {
            return sensitivityMap.get(speciesCheck);
          }

          @Override
          public void close() {}
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
          public List<String> getSensitiveDataFields() {
            HashSet<String> fields = new HashSet<>();
            for (Generalisation g : SensitiveDataInterpreterTest.this.generalisations)
              for (FieldAccessor accessor : g.getFields())
                fields.add(accessor.getField().qualifiedName());
            return new ArrayList<>(fields);
          }

          @Override
          public List<Generalisation> getGeneralisations() {
            return SensitiveDataInterpreterTest.this.generalisations;
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
          public SensitivityReport report(SensitivityQuery sensitivityQuery) {
            boolean sensitive =
                this.isSensitive(
                    sensitivityQuery.getScientificName(), sensitivityQuery.getTaxonId());
            SensitivityReport.SensitivityReportBuilder builder = SensitivityReport.builder();
            builder.sensitive(sensitive);
            builder.accessControl(false);
            builder.valid(true);
            builder.loadable(true);
            if (sensitive) {
              SensitivityInstance instance =
                  SensitivityInstance.builder()
                      .generalisation(new GeneralisationRule("10km"))
                      .authority("ALA")
                      .zone(SensitivityZone.builder().id("NSW").name("New South Wales").build())
                      .build();
              SensitiveTaxon taxon =
                  SensitiveTaxon.builder()
                      .scientificName(sensitivityQuery.getScientificName())
                      .taxonId(sensitivityQuery.getTaxonId())
                      .instances(Collections.singletonList(instance))
                      .build();
              ValidationReport vr =
                  ValidationReport.builder().category("Sensitive").taxon(taxon).build();
              builder.report(vr);
            }
            return builder.build();
          }

          @Override
          public SensitivityReport report(
              String scientificName,
              String taxonId,
              String dataResourceUid,
              String stateProvince,
              String country,
              List<String> zones) {
            SensitivityQuery query =
                SensitivityQuery.builder()
                    .scientificName(scientificName)
                    .taxonId(taxonId)
                    .dataResourceUid(dataResourceUid)
                    .stateProvince(stateProvince)
                    .country(country)
                    .zones(zones)
                    .build();
            return this.report(query);
          }

          @Override
          public SensitivityReport process(ProcessQuery sensitivityQuery) {
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
              Map<String, Object> original = new HashMap<>();
              original.put(decimalLatitude, sensitivityQuery.getProperties().get(decimalLatitude));
              original.put(
                  decimalLongitude, sensitivityQuery.getProperties().get(decimalLongitude));
              Map<String, Object> results = new HashMap<>();
              results.put(
                  SensitiveDataInterpreter.DATA_GENERALIZATIONS.getField().qualifiedName(),
                  "Test generalisation");
              results.put(
                  SensitiveDataInterpreter.GENERALISATION_TO_APPLY_IN_METRES
                      .getField()
                      .qualifiedName(),
                  "10000");
              results.put(
                  SensitiveDataInterpreter.GENERALISATION_IN_METRES.getField().qualifiedName(),
                  "10000");
              results.put(decimalLatitude, "-32.0");
              results.put(decimalLongitude, "150.0");
              builder.original(original);
              builder.updated(results);
              builder.report(ValidationReport.builder().category("Sensitive").build());
            }
            return builder.build();
          }
        };
    this.sensitive =
        this.conservationApi.getSensitiveDataFields().stream()
            .map(s -> TermFactory.instance().findTerm(s))
            .collect(Collectors.toSet());
    this.sensitivityReportLookup =
        new KeyValueStore<SensitivityQuery, SensitivityReport>() {
          @Override
          public SensitivityReport get(SensitivityQuery query) {
            return conservationApi.report(query);
          }

          @Override
          public void close() {}
        };
    this.sensitiveVocab = Sensitivity.getInstance(null);
  }

  @Test
  public void testConstructFields1() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.qualifiedName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.qualifiedName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, er);
    assertFalse(properties.isEmpty());
    for (Term term : this.sensitive) {
      String val = map.get(term.qualifiedName());
      assertEquals("Fields " + term + " don't match", val, properties.get(term.qualifiedName()));
    }
    for (String field : map.keySet()) {
      if (!this.sensitive.contains(TermFactory.instance().findTerm(field)))
        assertNull("Fields " + field + " should be absent", properties.get(field));
    }
  }

  @Test
  public void testConstructFields2() {
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
    SensitiveDataInterpreter.constructFields(sensitive, properties, lr);
    assertFalse(properties.isEmpty());
    for (Term field : this.sensitive) {
      if (lr.getSchema().getField(field.simpleName()) != null) {
        Object val = lr.get(field.simpleName());
        assertEquals(
            "Fields " + field + " don't match",
            val == null ? null : val.toString(),
            properties.get(field.qualifiedName()));
      }
    }
    for (Schema.Field field : lr.getSchema().getFields()) {
      if (!this.sensitive.contains(TermFactory.instance().findTerm(field.name())))
        assertNull("Fields " + field + " should be absent", properties.get(field.name()));
    }
  }

  @Test
  public void testConstructFields3() {
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
                RankedNameWithAuthorship.newBuilder()
                    .setKey("26")
                    .setRank(Rank.SPECIES.toString())
                    .setName("Acacia dealbata")
                    .build())
            .build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, tr);
    assertFalse(properties.isEmpty());
    assertEquals(2, properties.size());
    assertEquals("Acacia dealbata", properties.get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("26", properties.get(DwcTerm.taxonConceptID.qualifiedName()));
  }

  @Test
  public void testConstructFields4() {
    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId("1")
            .setEventDate(EventDate.newBuilder().setGte("2020-03-01").setLte("2020-03-02").build())
            .setDay(1)
            .setMonth(3)
            .setYear(2020)
            .build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, tr);
    assertFalse(properties.isEmpty());
    assertEquals(2, properties.size());
    assertEquals("2020-03-01", properties.get(DwcTerm.eventDate.qualifiedName()));
    assertEquals(
        "2020-03-02", properties.get(SensitiveDataInterpreter.EVENT_DATE_END_TERM.qualifiedName()));
  }

  @Test
  public void testConstructFields5() {
    ALATaxonRecord tr =
        ALATaxonRecord.newBuilder()
            .setId("1")
            .setScientificName("Acacia dealbata")
            .setTaxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .setKingdom("Plantae")
            .setScientificNameAuthorship("Link")
            .setTaxonRank("species")
            .build();
    Map<String, String> properties = new HashMap<>();
    Set<Term> sensitive =
        this.conservationApi.getSensitiveDataFields().stream()
            .map(s -> TermFactory.instance().findTerm(s))
            .collect(Collectors.toSet());
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, tr);
    assertFalse(properties.isEmpty());
    for (Term field : this.sensitive) {
      if (tr.getSchema().getField(field.simpleName()) != null) {
        Object val = tr.get(field.simpleName());
        assertEquals(
            "Fields " + field + " don't match",
            val == null ? null : val.toString(),
            properties.get(field.qualifiedName()));
      }
    }
    for (Schema.Field field : tr.getSchema().getFields()) {
      if (!this.sensitive.contains(TermFactory.instance().findTerm(field.name())))
        assertNull("Fields " + field + " should be absent", properties.get(field.name()));
    }
  }

  @Test
  public void testApplySensitivity1() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.qualifiedName(), "2020-01-01");
    map.put(IndexFields.EVENT_DATE_END, "2020-01-10");
    map.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.qualifiedName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.decimalLatitude.simpleName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.simpleName(), "149.6");
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(this.sensitive, sr, er);
    assertEquals("Acacia dealbata", er.getCoreTerms().get(DwcTerm.scientificName.qualifiedName()));
    assertEquals("2020-01-01", er.getCoreTerms().get(DwcTerm.eventDate.qualifiedName()));
    assertEquals("2020-01-10", er.getCoreTerms().get(IndexFields.EVENT_DATE_END));
    assertEquals("-39.8", er.getCoreTerms().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.6", er.getCoreTerms().get(DwcTerm.decimalLongitude.qualifiedName()));
  }

  @Test
  public void testApplySensitivity2() {
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
    properties.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.qualifiedName(), "149.6");
    properties.put(DwcTerm.locality.qualifiedName(), null);
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder()
            .setId("1")
            .setIsSensitive(true)
            .setAltered(properties)
            .build();
    SensitiveDataInterpreter.applySensitivity(this.sensitive, sr, lr);
    assertEquals(-39.8, lr.getDecimalLatitude(), 0.0001);
    assertEquals(149.6, lr.getDecimalLongitude(), 0.0001);
    assertNull(lr.getLocality());
  }

  @Test
  public void testApplySensitivity3() {
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
                RankedNameWithAuthorship.newBuilder()
                    .setKey("26")
                    .setRank(Rank.SPECIES.toString())
                    .setName("Acacia dealbata")
                    .build())
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.scientificName.simpleName(), "Acacia");
    properties.put(DwcTerm.decimalLatitude.simpleName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.simpleName(), "149.6");
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(this.sensitive, sr, tr);
    assertEquals("Acacia", tr.getAcceptedUsage().getName());
  }

  @Test
  public void testApplySensitivity4() {
    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId("1")
            .setEventDate(EventDate.newBuilder().setGte("2020-03-01").setLte("2020-03-02").build())
            .setDay(1)
            .setMonth(3)
            .setYear(2020)
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.scientificName.qualifiedName(), "Acacia");
    properties.put(DwcTerm.eventDate.qualifiedName(), "2020");
    properties.put(DwcTerm.year.qualifiedName(), "2020");
    properties.put(DwcTerm.month.qualifiedName(), null);
    properties.put(DwcTerm.day.qualifiedName(), null);
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder().setId("1").setAltered(properties).build();
    SensitiveDataInterpreter.applySensitivity(this.sensitive, sr, tr);
    assertEquals("2020", tr.getEventDate().getGte());
    assertEquals("2020", tr.getEventDate().getLte());
    assertEquals(2020, tr.getYear().intValue());
    assertNull(tr.getMonth());
    assertNull(tr.getDay());
  }

  @Test
  public void testApplySensitivity5() {
    ALATaxonRecord tr =
        ALATaxonRecord.newBuilder()
            .setId("1")
            .setScientificName("Acacia dealbata")
            .setTaxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .setKingdom("Plantae")
            .setScientificNameAuthorship("Link")
            .setTaxonRank("species")
            .build();
    Map<String, String> properties = new HashMap<>();
    properties.put(DwcTerm.scientificName.qualifiedName(), "Acacia");
    properties.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.8");
    properties.put(DwcTerm.decimalLongitude.qualifiedName(), "149.6");
    ALASensitivityRecord sr =
        ALASensitivityRecord.newBuilder()
            .setId("1")
            .setIsSensitive(true)
            .setAltered(properties)
            .build();
    SensitiveDataInterpreter.applySensitivity(this.sensitive, sr, tr);
    assertEquals("Acacia", tr.getScientificName());
    assertEquals("species", tr.getTaxonRank());
  }

  @Test
  public void testSourceQualityChecks1() {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.qualifiedName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.qualifiedName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, er);
    SensitiveDataInterpreter.sourceQualityChecks(properties, sr);
    assertTrue(sr.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void testSourceQualityChecks2() {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.eventDate.simpleName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.simpleName(), "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.simpleName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.simpleName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, er);
    SensitiveDataInterpreter.sourceQualityChecks(properties, sr);
    assertFalse(sr.getIssues().getIssueList().isEmpty());
    assertTrue(
        sr.getIssues().getIssueList().contains(ALAOccurrenceIssue.NAME_NOT_SUPPLIED.getId()));
  }

  @Test
  public void testSensitiveDataInterpreter1() {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.qualifiedName(), "2020-01-01");
    map.put(IndexFields.EVENT_DATE_END, "2020-01-10");
    map.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.qualifiedName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    Map<String, String> generalisations = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, er);
    SensitiveDataInterpreter.sensitiveDataInterpreter(
        this.sensitivityLookup,
        this.sensitivityReportLookup,
        this.generalisations,
        "dr1",
        properties,
        generalisations,
        this.sensitiveVocab,
        sr);
    assertTrue(sr.getIssues().getIssueList().isEmpty());
    assertTrue(sr.getIsSensitive());
    assertEquals("generalised", sr.getSensitive());
    assertEquals("Test generalisation", sr.getDataGeneralizations());
    assertEquals(6, sr.getAltered().size());
    assertEquals("-39.8", sr.getAltered().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.5", sr.getAltered().get(DwcTerm.decimalLongitude.qualifiedName()));
    assertNull(sr.getAltered().get(DwcTerm.eventDate.qualifiedName()));
    assertNull(sr.getAltered().get(SensitiveDataInterpreter.EVENT_DATE_END_TERM.qualifiedName()));
    assertEquals(6, sr.getOriginal().size());
    assertEquals("-39.78", sr.getOriginal().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.55", sr.getOriginal().get(DwcTerm.decimalLongitude.qualifiedName()));
    assertEquals("2020-01-01", sr.getOriginal().get(DwcTerm.eventDate.qualifiedName()));
    assertEquals(
        "2020-01-10",
        sr.getOriginal().get(SensitiveDataInterpreter.EVENT_DATE_END_TERM.qualifiedName()));
  }

  @Test
  public void testSensitiveDataInterpreter2() {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.qualifiedName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.qualifiedName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    Map<String, String> generalisations = new HashMap<>();
    generalisations.put(DwcTerm.dataGeneralizations.qualifiedName(), "generalised");
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, er);
    SensitiveDataInterpreter.sensitiveDataInterpreter(
        this.sensitivityLookup,
        this.sensitivityReportLookup,
        this.generalisations,
        "dr1",
        properties,
        generalisations,
        this.sensitiveVocab,
        sr);
    assertTrue(sr.getIssues().getIssueList().isEmpty());
    assertTrue(sr.getIsSensitive());
    assertEquals("alreadyGeneralised", sr.getSensitive());
    assertEquals("Test generalisation", sr.getDataGeneralizations());
    assertEquals(5, sr.getAltered().size());
    assertEquals("-39.8", sr.getAltered().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.5", sr.getAltered().get(DwcTerm.decimalLongitude.qualifiedName()));
    assertEquals(5, sr.getOriginal().size());
    assertEquals("-39.78", sr.getOriginal().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.55", sr.getOriginal().get(DwcTerm.decimalLongitude.qualifiedName()));
  }

  @Test
  public void testSensitiveDataInterpreter3() {
    this.generalisations =
        Arrays.asList(
            new RetainGeneralisation(DwcTerm.scientificName),
            new LatLongGeneralisation(DwcTerm.decimalLatitude, DwcTerm.decimalLongitude),
            new MessageGeneralisation(
                DwcTerm.dataGeneralizations,
                "Data is already generalised",
                true,
                MessageGeneralisation.Trigger.ANY),
            new AddingGeneralisation(DwcTerm.coordinateUncertaintyInMeters, true, true, 0));
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.qualifiedName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.78");
    map.put(DwcTerm.decimalLongitude.qualifiedName(), "149.55");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    Map<String, String> generalisations = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, er);
    SensitiveDataInterpreter.sensitiveDataInterpreter(
        this.sensitivityLookup,
        this.sensitivityReportLookup,
        this.generalisations,
        "dr1",
        properties,
        generalisations,
        this.sensitiveVocab,
        sr);
    assertTrue(sr.getIssues().getIssueList().isEmpty());
    assertTrue(sr.getIsSensitive());
    assertEquals("alreadyGeneralised", sr.getSensitive());
    assertEquals("Data is already generalised", sr.getDataGeneralizations());
    assertEquals(4, sr.getAltered().size());
    assertEquals("-39.8", sr.getAltered().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.5", sr.getAltered().get(DwcTerm.decimalLongitude.qualifiedName()));
    assertEquals(4, sr.getOriginal().size());
    assertEquals("-39.78", sr.getOriginal().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.55", sr.getOriginal().get(DwcTerm.decimalLongitude.qualifiedName()));
  }

  @Test
  public void testSensitiveDataInterpreter5() {
    ALASensitivityRecord sr = ALASensitivityRecord.newBuilder().setId("1").build();
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.eventDate.qualifiedName(), "2020-01-01");
    map.put(
        DwcTerm.taxonConceptID.qualifiedName(),
        "https://id.biodiversity.org.au/taxon/apni/51286863");
    map.put(DwcTerm.decimalLatitude.qualifiedName(), "-39.7");
    map.put(DwcTerm.decimalLongitude.qualifiedName(), "149.5");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    Map<String, String> properties = new HashMap<>();
    Map<String, String> generalisations = new HashMap<>();
    SensitiveDataInterpreter.constructFields(this.sensitive, properties, er);
    SensitiveDataInterpreter.sensitiveDataInterpreter(
        this.sensitivityLookup,
        this.sensitivityReportLookup,
        this.generalisations,
        "dr1",
        properties,
        generalisations,
        this.sensitiveVocab,
        sr);
    assertTrue(sr.getIssues().getIssueList().isEmpty());
    assertTrue(sr.getIsSensitive());
    assertEquals("alreadyGeneralised", sr.getSensitive());
    assertEquals("Test generalisation", sr.getDataGeneralizations());
    assertEquals(5, sr.getAltered().size());
    assertEquals("-39.7", sr.getAltered().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.5", sr.getAltered().get(DwcTerm.decimalLongitude.qualifiedName()));
    assertEquals(5, sr.getOriginal().size());
    assertEquals("-39.7", sr.getOriginal().get(DwcTerm.decimalLatitude.qualifiedName()));
    assertEquals("149.5", sr.getOriginal().get(DwcTerm.decimalLongitude.qualifiedName()));
  }
}
