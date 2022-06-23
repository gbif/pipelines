package au.org.ala.pipelines.interpreters;

import static org.junit.Assert.*;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.names.ws.api.NameSearch;
import au.org.ala.names.ws.api.NameUsageMatch;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.util.*;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.ALAMatchType;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.NameType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AlaTaxonomyInterpreterTest {
  private static final String DATARESOURCE_UID = "drTest";

  private ALACollectoryMetadata dataResource;
  private Map<NameSearch, NameUsageMatch> nameMap;
  private KeyValueStore<NameSearch, NameUsageMatch> lookup;
  private Map<String, Boolean> kingdomMap;
  private KeyValueStore<String, Boolean> kingdomLookup;

  @Before
  public void setUp() {
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
    Map<String, List<String>> hintMap = this.dataResource.getHintMap();
    this.nameMap = new HashMap<>();
    // Simple lookup
    NameSearch search =
        NameSearch.builder()
            .kingdom("Plantae")
            .scientificName("Acacia dealbata")
            .hints(hintMap)
            .build();
    NameUsageMatch match =
        NameUsageMatch.builder()
            .success(true)
            .taxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .kingdom("Plantae")
            .scientificName("Acacia dealbata")
            .family("Fabaceae")
            .rank("SPECIES")
            .matchType("exactMatch")
            .nameType("SCIENTIFIC")
            .issues(Collections.singletonList("noIssue"))
            .build();
    this.nameMap.put(search, match);
    // Full search
    search =
        NameSearch.builder()
            .kingdom("Plantae")
            .phylum("Charophyta")
            .clazz("Equisetopsida")
            .order("Fabales")
            .family("Fabaceae")
            .genus("Acacia")
            .specificEpithet("dealbata")
            .infraspecificEpithet("subalpina")
            .scientificName("Acacia dealbata subalpina")
            .scientificNameAuthorship("Tindale & Kodela")
            .rank("subspecies")
            .verbatimTaxonRank("SUBSPECIES")
            .vernacularName("Alpine Wattle")
            .hints(hintMap)
            .build();
    this.nameMap.put(search, match);
    // Plantae search
    search = NameSearch.builder().kingdom("Plantae").hints(hintMap).build();
    match =
        NameUsageMatch.builder()
            .success(true)
            .taxonConceptID("https://id.biodiversity.org.au/taxon/apni/51337710")
            .kingdom("Plantae")
            .scientificName("Plantae")
            .rank("KINGDOM")
            .matchType("exactMatch")
            .nameType("SCIENTIFIC")
            .issues(Collections.singletonList("noIssue"))
            .build();
    this.nameMap.put(search, match);
    // TaxonID lookup
    search = NameSearch.builder().kingdom("Plantae").taxonID("1234").hints(hintMap).build();
    match =
        NameUsageMatch.builder()
            .success(true)
            .taxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .kingdom("Plantae")
            .scientificName("Acacia dealbata")
            .family("Fabaceae")
            .rank("SPECIES")
            .matchType("exactMatch")
            .nameType("SCIENTIFIC")
            .issues(Collections.singletonList("noIssue"))
            .build();
    this.nameMap.put(search, match);

    // Full lookup
    search =
        NameSearch.builder()
            .kingdom("ANIMALIA")
            .family("MACROPODIDAE")
            .scientificName("Macropus rufus")
            .hints(hintMap)
            .build();
    match =
        NameUsageMatch.builder()
            .success(true)
            .taxonConceptID(
                "urn:lsid:biodiversity.org.au:afd.taxon:e6aff6af-ff36-4ad5-95f2-2dfdcca8caff")
            .scientificName("Osphranter rufus")
            .scientificNameAuthorship("(Desmarest, 1822)")
            .rank("SPECIES")
            .rankID(7000)
            .matchType("exactMatch")
            .nameType("SCIENTIFIC")
            .synonymType("SYNONYM")
            .lft(202407)
            .rgt(202407)
            .kingdom("ANIMALIA")
            .kingdomID(
                "urn:lsid:biodiversity.org.au:afd.taxon:4647863b-760d-4b59-aaa1-502c8cdf8d3c")
            .phylum("CHORDATA")
            .phylumID("urn:lsid:biodiversity.org.au:afd.taxon:065f1da4-53cd-40b8-a396-80fa5c74dedd")
            .classs("MAMMALIA")
            .classID("urn:lsid:biodiversity.org.au:afd.taxon:e9e7db31-04df-41fb-bd8d-e0b0f3c332d6")
            .order("DIPROTODONTIA")
            .orderID("urn:lsid:biodiversity.org.au:afd.taxon:bd223248-af12-4ce9-9380-4f9a85be38db")
            .family("MACROPODIDAE")
            .familyID("urn:lsid:biodiversity.org.au:afd.taxon:190ad4b1-0444-4791-96a5-ee514438d7e6")
            .genus("Osphranter")
            .genusID("urn:lsid:biodiversity.org.au:afd.taxon:288b19b6-1b3a-4746-aecd-5b2127aa2855")
            .species("Osphranter rufus")
            .speciesID(
                "urn:lsid:biodiversity.org.au:afd.taxon:e6aff6af-ff36-4ad5-95f2-2dfdcca8caff")
            .issues(Collections.singletonList("homonym"))
            .vernacularName("Red Kangaroo")
            .speciesGroup(Arrays.asList("Animals", "Mammals"))
            .speciesSubgroup(Collections.singletonList("Herbivorous Marsupials"))
            .build();
    this.nameMap.put(search, match);
    this.lookup =
        new KeyValueStore<NameSearch, NameUsageMatch>() {
          @Override
          public void close() {}

          @Override
          public NameUsageMatch get(NameSearch o) {
            return nameMap.getOrDefault(o, NameUsageMatch.FAIL);
          }
        };
    this.kingdomMap = new HashMap<>();
    this.kingdomMap.put("Animalia", true);
    this.kingdomMap.put("Gronk", false);
    this.kingdomLookup =
        new KeyValueStore<String, Boolean>() {
          @Override
          public void close() {}

          @Override
          public Boolean get(String o) {
            return kingdomMap.get(o);
          }
        };
  }

  @After
  public void tearDown() throws Exception {
    this.lookup.close();
  }

  // Test with default value
  @Test
  public void testMatch1() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    assertEquals("https://id.biodiversity.org.au/taxon/apni/51286863", atr.getTaxonConceptID());
    assertEquals("Acacia dealbata", atr.getScientificName());
    assertEquals("Fabaceae", atr.getFamily());
    assertEquals("Plantae", atr.getKingdom());
    assertEquals("exactMatch", atr.getMatchType());
    assertEquals("SCIENTIFIC", atr.getNameType());
    assertTrue(atr.getIssues().getIssueList().isEmpty());
  }

  // Test with explicit value
  @Test
  public void testMatch2() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.kingdom.qualifiedName(), "Plantae");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    assertEquals("https://id.biodiversity.org.au/taxon/apni/51286863", atr.getTaxonConceptID());
    assertEquals("Acacia dealbata", atr.getScientificName());
    assertEquals("Fabaceae", atr.getFamily());
    assertEquals("Plantae", atr.getKingdom());
    assertEquals("exactMatch", atr.getMatchType());
    assertEquals("SCIENTIFIC", atr.getNameType());
    assertTrue(atr.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void testMatch3() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Macropus rufus");
    map.put(DwcTerm.kingdom.qualifiedName(), "ANIMALIA");
    map.put(DwcTerm.family.qualifiedName(), "MACROPODIDAE");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:e6aff6af-ff36-4ad5-95f2-2dfdcca8caff",
        atr.getTaxonConceptID());
    assertEquals("Osphranter rufus", atr.getScientificName());
    assertEquals("(Desmarest, 1822)", atr.getScientificNameAuthorship());
    assertEquals("SPECIES", atr.getTaxonRank());
    assertEquals(7000, (int) atr.getTaxonRankID());
    assertEquals("exactMatch", atr.getMatchType());
    assertEquals("SCIENTIFIC", atr.getNameType());
    assertEquals(202407, (int) atr.getLft());
    assertEquals(202407, (int) atr.getRgt());
    assertEquals("ANIMALIA", atr.getKingdom());
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:4647863b-760d-4b59-aaa1-502c8cdf8d3c",
        atr.getKingdomID());
    assertEquals("CHORDATA", atr.getPhylum());
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:065f1da4-53cd-40b8-a396-80fa5c74dedd",
        atr.getPhylumID());
    assertEquals("MAMMALIA", atr.getClasss());
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:e9e7db31-04df-41fb-bd8d-e0b0f3c332d6",
        atr.getClassID());
    assertEquals("DIPROTODONTIA", atr.getOrder());
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:bd223248-af12-4ce9-9380-4f9a85be38db",
        atr.getOrderID());
    assertEquals("MACROPODIDAE", atr.getFamily());
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:190ad4b1-0444-4791-96a5-ee514438d7e6",
        atr.getFamilyID());
    assertEquals("Osphranter", atr.getGenus());
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:288b19b6-1b3a-4746-aecd-5b2127aa2855",
        atr.getGenusID());
    assertEquals("Osphranter rufus", atr.getSpecies());
    assertEquals(
        "urn:lsid:biodiversity.org.au:afd.taxon:e6aff6af-ff36-4ad5-95f2-2dfdcca8caff",
        atr.getSpeciesID());
    assertTrue(atr.getIssues().getIssueList().contains("TAXON_HOMONYM"));
    assertEquals("Red Kangaroo", atr.getVernacularName());
    assertTrue(atr.getSpeciesGroup().contains("Animals"));
    assertTrue(atr.getSpeciesGroup().contains("Mammals"));
    assertTrue(atr.getSpeciesSubgroup().contains("Herbivorous Marsupials"));
  }

  @Test
  public void testMatch4() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.kingdom.qualifiedName(), "Plantae");
    map.put(DwcTerm.phylum.qualifiedName(), "Charophyta");
    map.put(DwcTerm.class_.qualifiedName(), "Equisetopsida");
    map.put(DwcTerm.order.qualifiedName(), "Fabales");
    map.put(DwcTerm.family.qualifiedName(), "Fabaceae");
    map.put(DwcTerm.genus.qualifiedName(), "Acacia");
    map.put(DwcTerm.specificEpithet.qualifiedName(), "dealbata");
    map.put(DwcTerm.infraspecificEpithet.qualifiedName(), "subalpina");
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata subalpina");
    map.put(DwcTerm.scientificNameAuthorship.qualifiedName(), "Tindale & Kodela");
    map.put(DwcTerm.taxonRank.qualifiedName(), "subspecies");
    map.put(DwcTerm.verbatimTaxonRank.qualifiedName(), "SUBSPECIES");
    map.put(DwcTerm.vernacularName.qualifiedName(), "Alpine Wattle");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    assertEquals("https://id.biodiversity.org.au/taxon/apni/51286863", atr.getTaxonConceptID());
    assertEquals("Acacia dealbata", atr.getScientificName());
    assertEquals("Fabaceae", atr.getFamily());
    assertEquals("Plantae", atr.getKingdom());
    assertEquals("exactMatch", atr.getMatchType());
    assertEquals("SCIENTIFIC", atr.getNameType());
    assertTrue(atr.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void testMatchOnTaxonID() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.taxonID.qualifiedName(), "1234");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, true)
        .accept(er, atr);
    assertEquals("https://id.biodiversity.org.au/taxon/apni/51286863", atr.getTaxonConceptID());
    assertEquals("Acacia dealbata", atr.getScientificName());
    assertEquals("Fabaceae", atr.getFamily());
    assertEquals("Plantae", atr.getKingdom());
    assertEquals("exactMatch", atr.getMatchType());
    assertEquals("SCIENTIFIC", atr.getNameType());
    assertTrue(atr.getIssues().getIssueList().isEmpty());
  }

  @Test
  public void testNoMatch1() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Vombatus ursinus");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    assertNull(atr.getTaxonConceptID());
    assertNull(atr.getMatchType());
    assertFalse(atr.getIssues().getIssueList().contains("noMatch"));
    assertTrue(atr.getIssues().getIssueList().contains("TAXON_MATCH_NONE"));
  }

  @Test
  public void testNoMatch2() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    map.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    assertNull(atr.getTaxonConceptID());
    assertNull(atr.getMatchType());
    assertFalse(atr.getIssues().getIssueList().contains("noMatch"));
    assertTrue(atr.getIssues().getIssueList().contains("TAXON_MATCH_NONE"));
  }

  @Test
  public void testSourceCheck1() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaSourceQualityChecks(this.dataResource, this.kingdomLookup)
        .accept(er, atr);
    assertEquals(
        Collections.singletonList(ALAOccurrenceIssue.MISSING_TAXONRANK.name()),
        atr.getIssues().getIssueList());
  }

  @Test
  public void testSourceCheck2() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Ospranter rufus");
    map.put(DwcTerm.taxonRank.qualifiedName(), "species");
    map.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaSourceQualityChecks(this.dataResource, this.kingdomLookup)
        .accept(er, atr);
    assertEquals(Collections.emptyList(), atr.getIssues().getIssueList());
  }

  @Test
  public void testSourceCheck3() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Ospranter rufus");
    map.put(DwcTerm.kingdom.qualifiedName(), "Gronk");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaSourceQualityChecks(this.dataResource, this.kingdomLookup)
        .accept(er, atr);
    assertEquals(
        Arrays.asList(
            ALAOccurrenceIssue.MISSING_TAXONRANK.name(), ALAOccurrenceIssue.UNKNOWN_KINGDOM.name()),
        atr.getIssues().getIssueList());
  }

  @Test
  public void testSourceCheck4() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaSourceQualityChecks(this.dataResource, this.kingdomLookup)
        .accept(er, atr);
    assertEquals(
        Arrays.asList(
            ALAOccurrenceIssue.MISSING_TAXONRANK.name(),
            ALAOccurrenceIssue.NAME_NOT_SUPPLIED.name()),
        atr.getIssues().getIssueList());
  }

  @Test
  public void testResultCheck1() {
    Map<String, String> map = new HashMap<>();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata");
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    ALATaxonomyInterpreter.alaResultQualityChecks(this.dataResource).accept(er, atr);
    assertEquals(Collections.emptyList(), atr.getIssues().getIssueList());
  }

  @Test
  public void testResultCheck2() {
    Map<String, String> map = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    ALATaxonRecord atr = ALATaxonRecord.newBuilder().setId("1").build();
    ALATaxonomyInterpreter.alaTaxonomyInterpreter(this.dataResource, this.lookup, false)
        .accept(er, atr);
    ALATaxonomyInterpreter.alaResultQualityChecks(this.dataResource).accept(er, atr);
    assertEquals(
        Collections.singletonList(ALAOccurrenceIssue.TAXON_DEFAULT_MATCH.name()),
        atr.getIssues().getIssueList());
  }

  @Test
  public void testResultCheck3() {
    Map<String, String> map = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia dealbata nothingosis");
    ALATaxonRecord atr =
        ALATaxonRecord.newBuilder()
            .setId("1")
            .setTaxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .setScientificName("Acacia dealbata")
            .setTaxonRank("SPECIES")
            .setMatchType(ALAMatchType.higherMatch.name())
            .build();
    ALATaxonomyInterpreter.alaResultQualityChecks(this.dataResource).accept(er, atr);
    assertEquals(
        Collections.singletonList(OccurrenceIssue.TAXON_MATCH_HIGHERRANK.name()),
        atr.getIssues().getIssueList());
  }

  @Test
  public void testResultCheck4() {
    Map<String, String> map = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    map.put(DwcTerm.scientificName.qualifiedName(), "Akacia dealbati");
    ALATaxonRecord atr =
        ALATaxonRecord.newBuilder()
            .setId("1")
            .setTaxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .setScientificName("Acacia dealbata")
            .setTaxonRank("SPECIES")
            .setMatchType(ALAMatchType.fuzzyMatch.name())
            .build();
    ALATaxonomyInterpreter.alaResultQualityChecks(this.dataResource).accept(er, atr);
    assertEquals(
        Collections.singletonList(OccurrenceIssue.TAXON_MATCH_FUZZY.name()),
        atr.getIssues().getIssueList());
  }

  @Test
  public void testResultCheck5() {
    Map<String, String> map = new HashMap<>();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("1").setCoreTerms(map).build();
    map.put(DwcTerm.scientificName.qualifiedName(), "Acacia sp. 1");
    ALATaxonRecord atr =
        ALATaxonRecord.newBuilder()
            .setId("1")
            .setTaxonConceptID("https://id.biodiversity.org.au/taxon/apni/51286863")
            .setScientificName("Acacia dealbata")
            .setTaxonRank("SPECIES")
            .setNameType(NameType.PLACEHOLDER.name())
            .build();
    ALATaxonomyInterpreter.alaResultQualityChecks(this.dataResource).accept(er, atr);
    assertEquals(
        Collections.singletonList(ALAOccurrenceIssue.INVALID_SCIENTIFIC_NAME.name()),
        atr.getIssues().getIssueList());
  }
}
