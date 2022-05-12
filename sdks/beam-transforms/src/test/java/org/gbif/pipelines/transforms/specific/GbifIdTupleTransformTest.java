package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class GbifIdTupleTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void tupleTest() {

    // State
    GbifIdRecord inGir = GbifIdRecord.newBuilder().setId("777").setGbifId(1L).build();

    GbifIdRecord inIssueGir = GbifIdRecord.newBuilder().setId("888").build();
    addIssue(inIssueGir, GBIF_ID_ABSENT);

    // When
    GbifIdTupleTransform tupleTransform = GbifIdTupleTransform.create();

    PCollectionTuple girTuple =
        p.apply("Read GbifIdRecord", Create.of(inGir, inIssueGir))
            .apply("Interpret IDs", tupleTransform);

    // Should
    PCollection<GbifIdRecord> girCollection = girTuple.get(tupleTransform.getTag());
    PCollection<GbifIdRecord> absentGirCollection = girTuple.get(tupleTransform.getAbsentTag());

    PAssert.that(girCollection).containsInAnyOrder(inGir);
    PAssert.that(absentGirCollection).containsInAnyOrder(inIssueGir);
    p.run();
  }
}
