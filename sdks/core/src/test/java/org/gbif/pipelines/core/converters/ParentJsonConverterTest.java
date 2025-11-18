package org.gbif.pipelines.core.converters;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.vocabulary.DurationUnit;
import org.gbif.api.vocabulary.EventIssue;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonHumboldtRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.HumboldtTaxonClassification;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.ParentJsonRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.junit.Test;

public class ParentJsonConverterTest {

  private static final String CHECKLIST_KEY = UUID.randomUUID().toString();
  private static final String CHECKLIST_KEY2 = UUID.randomUUID().toString();

  @Test
  public void basicHumboldtConverterTest() {
    HumboldtRecord hr =
        HumboldtRecord.newBuilder()
            .setId("777")
            .setHumboldtItems(
                Collections.singletonList(
                    Humboldt.newBuilder()
                        .setSiteCount(2)
                        .setTargetHabitatScope(Arrays.asList("hs1", "hs2"))
                        .setExcludedHabitatScope(Collections.singletonList("hs1"))
                        .setEventDurationUnit(DurationUnit.HOURS.name())
                        .setEventDurationValue(2.0)
                        .setGeospatialScopeAreaValue(12.0)
                        .setTargetLifeStageScope(
                            Collections.singletonList(
                                VocabularyConcept.newBuilder()
                                    .setConcept("lf1")
                                    .setLineage(Collections.singletonList("lf1"))
                                    .build()))
                        .setTargetTaxonomicScope(
                            Collections.singletonList(
                                TaxonHumboldtRecord.newBuilder()
                                    .setChecklistKey(CHECKLIST_KEY)
                                    .setUsageKey("k1")
                                    .setUsageName("n1")
                                    .setUsageRank("r1")
                                    .setClassification(
                                        Arrays.asList(
                                            RankedName.newBuilder()
                                                .setKey("k1")
                                                .setName("n1")
                                                .setRank("r1")
                                                .build(),
                                            RankedName.newBuilder()
                                                .setKey("k2")
                                                .setName("n2")
                                                .setRank("r2")
                                                .build()))
                                    .setIssues(
                                        IssueRecord.newBuilder()
                                            .setIssueList(
                                                List.of(OccurrenceIssue.TAXON_MATCH_NONE.name()))
                                            .build())
                                    .build()))
                        .build()))
            .setIssues(
                IssueRecord.newBuilder()
                    .setIssueList(
                        List.of(
                            EventIssue.TARGET_DEGREE_OF_ESTABLISHMENT_EXCLUDED.name(),
                            EventIssue.HAS_MATERIAL_SAMPLES_MISMATCH.name()))
                    .build())
            .build();

    ParentJsonRecord parentJsonRecord =
        createBaseConverter().humboldtRecord(hr).build().convertToParent();

    List<org.gbif.pipelines.io.avro.json.Humboldt> humboldtJsonList =
        parentJsonRecord.getEvent().getHumboldt();
    assertEquals(1, humboldtJsonList.size());

    assertEquals(2, parentJsonRecord.getEvent().getIssues().size());
    org.gbif.pipelines.io.avro.json.Humboldt first = humboldtJsonList.get(0);
    assertEquals(2, first.getSiteCount().intValue());
    assertEquals(2, first.getTargetHabitatScope().size());
    assertEquals(1, first.getExcludedHabitatScope().size());
    assertEquals(120.0, first.getEventDurationValueInMinutes(), 0.0001);
    assertEquals(1, first.getTargetLifeStageScope().size());
    assertTrue(first.getExcludedLifeStageScope().isEmpty());
    assertEquals(12.0, first.getGeospatialScopeAreaValue(), 0.0001);
    assertNull(first.getGeospatialScopeAreaUnit());

    Map<String, List<HumboldtTaxonClassification>> taxonScope = first.getTargetTaxonomicScope();
    assertEquals(1, taxonScope.size());
    assertTrue(taxonScope.containsKey(CHECKLIST_KEY));
    List<HumboldtTaxonClassification> classification = taxonScope.get(CHECKLIST_KEY);
    assertEquals(1, classification.size());
    HumboldtTaxonClassification humboldtTaxonClassification = classification.get(0);
    assertEquals("k1", humboldtTaxonClassification.getUsageKey());
    assertEquals("n1", humboldtTaxonClassification.getUsageName());
    assertEquals("r1", humboldtTaxonClassification.getUsageRank());
    assertEquals(2, humboldtTaxonClassification.getClassification().size());
    assertEquals(2, humboldtTaxonClassification.getClassificationKeys().size());
    assertEquals(2, humboldtTaxonClassification.getTaxonKeys().size());
    assertTrue(humboldtTaxonClassification.getTaxonKeys().contains("k1"));
    assertTrue(humboldtTaxonClassification.getTaxonKeys().contains("k2"));
    assertEquals(1, humboldtTaxonClassification.getIssues().size());
  }

  @Test
  public void multipleHumboldtTest() {
    HumboldtRecord hr =
        HumboldtRecord.newBuilder()
            .setId("777")
            .setHumboldtItems(
                Arrays.asList(
                    Humboldt.newBuilder().setSiteCount(2).build(),
                    Humboldt.newBuilder().setSiteCount(5).build()))
            .build();

    ParentJsonRecord parentJsonRecord =
        createBaseConverter().humboldtRecord(hr).build().convertToParent();

    List<org.gbif.pipelines.io.avro.json.Humboldt> humboldtJsonList =
        parentJsonRecord.getEvent().getHumboldt();
    assertEquals(2, humboldtJsonList.size());
    assertEquals(1, humboldtJsonList.stream().filter(h -> h.getSiteCount() == 2).count());
    assertEquals(1, humboldtJsonList.stream().filter(h -> h.getSiteCount() == 5).count());
  }

  @Test
  public void multipleTaxaHumboldtTest() {
    HumboldtRecord hr =
        HumboldtRecord.newBuilder()
            .setId("777")
            .setHumboldtItems(
                Collections.singletonList(
                    Humboldt.newBuilder()
                        .setTargetTaxonomicScope(
                            Arrays.asList(
                                taxonHumboldtRecord(CHECKLIST_KEY, "1", "2"),
                                taxonHumboldtRecord(CHECKLIST_KEY2, "11", "22"),
                                taxonHumboldtRecord(CHECKLIST_KEY, "30", "20"),
                                taxonHumboldtRecord(CHECKLIST_KEY2, "301", "201")))
                        .build()))
            .build();

    ParentJsonRecord parentJsonRecord =
        createBaseConverter().humboldtRecord(hr).build().convertToParent();

    List<org.gbif.pipelines.io.avro.json.Humboldt> humboldtJsonList =
        parentJsonRecord.getEvent().getHumboldt();
    assertEquals(1, humboldtJsonList.size());
    Map<String, List<HumboldtTaxonClassification>> taxon =
        humboldtJsonList.get(0).getTargetTaxonomicScope();
    assertEquals(2, taxon.size());
    assertEquals(2, taxon.get(CHECKLIST_KEY).size());
    assertEquals(2, taxon.get(CHECKLIST_KEY2).size());
    assertEquals(
        1, taxon.get(CHECKLIST_KEY).stream().filter(t -> t.getUsageKey().equals("k1")).count());

    assertEquals(
        1, taxon.get(CHECKLIST_KEY2).stream().filter(t -> t.getUsageKey().equals("k11")).count());
    assertEquals(
        1, taxon.get(CHECKLIST_KEY).stream().filter(t -> t.getUsageKey().equals("k30")).count());
    assertEquals(
        1, taxon.get(CHECKLIST_KEY2).stream().filter(t -> t.getUsageKey().equals("k301")).count());
  }

  private static TaxonHumboldtRecord taxonHumboldtRecord(
      String checklistKey, String name, String parent) {
    return TaxonHumboldtRecord.newBuilder()
        .setChecklistKey(checklistKey)
        .setUsageKey("k" + name)
        .setUsageName("n" + name)
        .setUsageRank("r" + name)
        .setClassification(
            Arrays.asList(
                RankedName.newBuilder()
                    .setKey("k" + name)
                    .setName("n" + name)
                    .setRank("r" + name)
                    .build(),
                RankedName.newBuilder()
                    .setKey("k" + parent)
                    .setName("n" + parent)
                    .setRank("r" + parent)
                    .build()))
        .build();
  }

  private static ParentJsonConverter.ParentJsonConverterBuilder createBaseConverter() {
    return ParentJsonConverter.builder()
        .metadata(MetadataRecord.newBuilder().setId("1").setCrawlId(1).build())
        .verbatim(ExtendedRecord.newBuilder().setId("1").build())
        .identifier(IdentifierRecord.newBuilder().setId("1").setInternalId("1").build())
        .eventCore(EventCoreRecord.newBuilder().setId("1").build())
        .temporal(TemporalRecord.newBuilder().setId("1").build())
        .location(LocationRecord.newBuilder().setId("1").build())
        .multimedia(MultimediaRecord.newBuilder().setId("1").build())
        .measurementOrFactRecord(MeasurementOrFactRecord.newBuilder().setId("1").build())
        .locationInheritedRecord(LocationInheritedRecord.newBuilder().setId("1").build())
        .temporalInheritedRecord(TemporalInheritedRecord.newBuilder().setId("1").build())
        .eventInheritedRecord(EventInheritedRecord.newBuilder().setId("1").build());
  }
}
