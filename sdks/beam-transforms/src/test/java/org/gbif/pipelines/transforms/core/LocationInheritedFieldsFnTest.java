package org.gbif.pipelines.transforms.core;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.junit.Test;

/** Tests for LocationInheritedFieldsFn. */
public class LocationInheritedFieldsFnTest {

  @Test
  public void testAcc() {
    // Creates the combine function
    LocationInheritedFieldsFn locationInheritedFieldsFn = new LocationInheritedFieldsFn();

    // Accumulates 2 records in one accumulator
    LocationInheritedFieldsFn.Accum accum1 = locationInheritedFieldsFn.createAccumulator();
    locationInheritedFieldsFn.addInput(
        accum1,
        LocationRecord.newBuilder()
            .setId("1")
            .setHasCoordinate(true)
            .setDecimalLatitude(0.0d)
            .setDecimalLongitude(90.0d)
            .build());

    locationInheritedFieldsFn.addInput(
        accum1,
        LocationRecord.newBuilder()
            .setId("2")
            .setParentId("1")
            .setHasCoordinate(true)
            .setDecimalLatitude(1.0d)
            .setDecimalLongitude(91.0d)
            .build());

    // Accumulates one leaf record in a different accumulator
    LocationInheritedFieldsFn.Accum accum2 = locationInheritedFieldsFn.createAccumulator();
    locationInheritedFieldsFn.addInput(
        accum2, LocationRecord.newBuilder().setId("3").setParentId("2").build());

    // Merge accumulators
    LocationInheritedFieldsFn.Accum mergedAccum =
        locationInheritedFieldsFn.mergeAccumulators(Arrays.asList(accum1, accum2));

    // Get the results
    LocationInheritedRecord locationInheritedRecord =
        locationInheritedFieldsFn.extractOutput(mergedAccum);

    // Results are gotten from the immediate parent of the leaf record
    assertEquals(new Double(1.0d), locationInheritedRecord.getDecimalLatitude());
    assertEquals(new Double(91.0d), locationInheritedRecord.getDecimalLongitude());
  }
}
