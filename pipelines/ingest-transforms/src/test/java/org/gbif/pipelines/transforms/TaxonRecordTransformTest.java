package org.gbif.pipelines.transforms;

import java.util.Collections;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.RankedName;
import org.gbif.api.vocabulary.Rank;
import org.gbif.kvs.species.SpeciesMatchRequest;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.parsers.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.transforms.core.TaxonomyTransform.Interpreter;
import org.gbif.rest.client.species.NameUsageMatch;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.gbif.api.vocabulary.TaxonomicStatus.ACCEPTED;

@Ignore("java.io.NotSerializableException: org.gbif.rest.client.species.NameUsageMatch")
@RunWith(JUnit4.class)
public class TaxonRecordTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void transformationTest() {

    KeyValueTestStore<SpeciesMatchRequest, NameUsageMatch> kvStore = new KeyValueTestStore<>();
    kvStore.put(SpeciesMatchRequest.builder().withScientificName("foo").build(), createDummyNameUsageMatch());

    // State
    ExtendedRecord extendedRecord = ExtendedRecordCustomBuilder.create().id("1").name("foo").build();

    // When
    PCollection<TaxonRecord> recordCollection =
        p.apply(Create.of(extendedRecord)).apply(ParDo.of(new Interpreter(kvStore)));

    // Should
    PAssert.that(recordCollection).containsInAnyOrder(createTaxonRecordExpected());

    p.run();
  }

  private TaxonRecord createTaxonRecordExpected() {
    TaxonRecord taxonRecord = TaxonRecord.newBuilder().build();
    TaxonRecordConverter.convert(createDummyNameUsageMatch(), taxonRecord);
    taxonRecord.setId("1");
    return taxonRecord;
  }

  private NameUsageMatch createDummyNameUsageMatch() {
    NameUsageMatch nameUsageMatch = new NameUsageMatch();

    RankedName usage = new RankedName();
    usage.setKey(123);
    usage.setName("test");
    usage.setRank(Rank.SPECIES);
    nameUsageMatch.setUsage(usage);

    RankedName kingdom = new RankedName();
    kingdom.setKey(1);
    kingdom.setName("Animalia");
    kingdom.setRank(Rank.KINGDOM);
    nameUsageMatch.setClassification(Collections.singletonList(kingdom));

    NameUsageMatch.Diagnostics diagnostics = new NameUsageMatch.Diagnostics();
    diagnostics.setMatchType(MatchType.EXACT);
    diagnostics.setStatus(ACCEPTED);
    nameUsageMatch.setDiagnostics(diagnostics);

    return nameUsageMatch;
  }
}
