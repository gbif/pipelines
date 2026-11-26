package org.gbif.pipelines.core.converters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.RankedNameWithAuthorship;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.json.Classification;
import org.junit.Test;

public class JsonConverterTest {

  private static final String CHECKLIST_KEY = "7ddf754f-d193-4cc9-b351-99906754a03b";

  private static final IssueRecord NO_MATCH =
      IssueRecord.newBuilder()
          .setIssueList(List.of(OccurrenceIssue.TAXON_MATCH_NONE.name()))
          .build();

  /** A match wins over a record without one of the same checklist, whatever their order. */
  @Test
  public void matchWinsOverNoMatchTest() {
    TaxonRecord match =
        TaxonRecord.newBuilder()
            .setDatasetKey(CHECKLIST_KEY)
            .setUsage(
                RankedNameWithAuthorship.newBuilder()
                    .setKey("10")
                    .setName("Puma concolor")
                    .setRank("SPECIES")
                    .build())
            .build();
    TaxonRecord noMatch =
        TaxonRecord.newBuilder().setDatasetKey(CHECKLIST_KEY).setIssues(NO_MATCH).build();

    for (List<TaxonRecord> records : List.of(List.of(noMatch, match), List.of(match, noMatch))) {
      Classification classification =
          JsonConverter.convertToClassifications(records).get(CHECKLIST_KEY);
      assertNotNull(classification.getUsage());
      assertEquals("10", classification.getUsage().getKey());
    }
  }

  /** Without a match, only the issues are kept, and without issues there's nothing to keep. */
  @Test
  public void noMatchOnlyKeepsIssuesTest() {
    TaxonRecord noMatch =
        TaxonRecord.newBuilder().setDatasetKey(CHECKLIST_KEY).setIssues(NO_MATCH).build();

    Map<String, Classification> classifications =
        JsonConverter.convertToClassifications(List.of(noMatch));
    assertEquals(
        List.of(OccurrenceIssue.TAXON_MATCH_NONE.name()),
        classifications.get(CHECKLIST_KEY).getIssues());
    assertTrue(classifications.get(CHECKLIST_KEY).getTaxonKeys().isEmpty());

    assertTrue(
        JsonConverter.convertToClassifications(
                List.of(TaxonRecord.newBuilder().setDatasetKey(CHECKLIST_KEY).build()))
            .isEmpty());
  }
}
