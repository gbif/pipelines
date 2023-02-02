package org.gbif.pipelines.transforms.specific;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Identifier.GBIF_ID_ABSENT;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssue;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.gbif.pipelines.io.avro.IdentifierRecord;
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
    IdentifierRecord inIr = IdentifierRecord.newBuilder().setId("777").setInternalId("1").build();

    IdentifierRecord inIssueIr = IdentifierRecord.newBuilder().setId("888").build();
    addIssue(inIssueIr, GBIF_ID_ABSENT);

    // When
    GbifIdTupleTransform tupleTransform = GbifIdTupleTransform.create();

    PCollectionTuple irTuple =
        p.apply("Read IdentifierRecord", Create.of(inIr, inIssueIr))
            .apply("Interpret IDs", tupleTransform);

    // Should
    PCollection<IdentifierRecord> irCollection = irTuple.get(tupleTransform.getTag());
    PCollection<IdentifierRecord> absentIrCollection = irTuple.get(tupleTransform.getAbsentTag());

    PAssert.that(irCollection).containsInAnyOrder(inIr);
    PAssert.that(absentIrCollection).containsInAnyOrder(inIssueIr);
    p.run();
  }
}
