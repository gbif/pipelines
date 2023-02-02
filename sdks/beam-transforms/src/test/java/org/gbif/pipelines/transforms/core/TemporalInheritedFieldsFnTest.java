package org.gbif.pipelines.transforms.core;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.junit.Test;

/** Tests for TemporalInheritedFieldsFn. */
public class TemporalInheritedFieldsFnTest {

  @Test
  public void testAcc() {

    // Creates the function
    TemporalInheritedFieldsFn temporalInheritedFieldsFn = new TemporalInheritedFieldsFn();

    // Accumulates 2 records in one accumulator
    TemporalInheritedFieldsFn.Accum accum1 = temporalInheritedFieldsFn.createAccumulator();
    temporalInheritedFieldsFn.addInput(
        accum1, TemporalRecord.newBuilder().setId("1").setYear(2020).setMonth(5).build());

    temporalInheritedFieldsFn.addInput(
        accum1,
        TemporalRecord.newBuilder().setId("2").setParentId("1").setYear(2022).setMonth(6).build());

    // Accumulates 1 leaf record in a second accumulator
    TemporalInheritedFieldsFn.Accum accum2 = temporalInheritedFieldsFn.createAccumulator();
    temporalInheritedFieldsFn.addInput(
        accum2, TemporalRecord.newBuilder().setId("3").setParentId("2").build());

    // Merge
    TemporalInheritedFieldsFn.Accum mergedAccum =
        temporalInheritedFieldsFn.mergeAccumulators(Arrays.asList(accum1, accum2));

    // Get the result
    TemporalInheritedRecord temporalInheritedRecord =
        temporalInheritedFieldsFn.extractOutput(mergedAccum);

    // Results are from the immediate parent
    assertEquals(new Integer(2022), temporalInheritedRecord.getYear());
    assertEquals(new Integer(6), temporalInheritedRecord.getMonth());
  }
}
