package org.gbif.pipelines.transforms.core;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.Parent;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.junit.Test;

/** Tests for EventInheritedFieldsFn. */
public class EventInheritedFieldsFnTest {

  @Test
  public void testAcc() {

    // Creates the function
    EventInheritedFieldsFn eventInheritedFieldsFn = new EventInheritedFieldsFn();

    // Accumulates 2 records in one accumulator
    EventInheritedFieldsFn.Accum accum1 = eventInheritedFieldsFn.createAccumulator();
    eventInheritedFieldsFn.addInput(
        accum1,
        EventCoreRecord.newBuilder()
            .setId("1")
            .setLocationID("L1")
            .setEventType(
                VocabularyConcept.newBuilder()
                    .setConcept("survey")
                    .setLineage(Collections.emptyList())
                    .build())
            .build());

    eventInheritedFieldsFn.addInput(
        accum1,
        EventCoreRecord.newBuilder()
            .setId("2")
            .setLocationID("L2")
            .setParentEventID("1")
            .setParentsLineage(
                List.of(Parent.newBuilder().setId("1").setEventType("survey").build()))
            .setEventType(
                VocabularyConcept.newBuilder()
                    .setConcept("sampling")
                    .setLineage(Collections.emptyList())
                    .build())
            .build());

    // Accumulates 1 leaf record in a second accumulator
    EventInheritedFieldsFn.Accum accum2 = eventInheritedFieldsFn.createAccumulator();
    eventInheritedFieldsFn.addInput(
        accum2,
        EventCoreRecord.newBuilder()
            .setId("3")
            .setParentEventID("2")
            .setParentsLineage(
                List.of(
                    Parent.newBuilder().setId("2").setEventType("sampling").build(),
                    Parent.newBuilder().setId("1").setEventType("survey").build()))
            .build());

    // Merge
    EventInheritedFieldsFn.Accum mergedAccum =
        eventInheritedFieldsFn.mergeAccumulators(List.of(accum1, accum2));

    // Get the result
    EventInheritedRecord eventInheritedRecord = eventInheritedFieldsFn.extractOutput(mergedAccum);

    // Results are from the immediate parent
    assertEquals(List.of("sampling", "survey"), eventInheritedRecord.getEventType());
    assertEquals("L2", eventInheritedRecord.getLocationID());
  }
}
