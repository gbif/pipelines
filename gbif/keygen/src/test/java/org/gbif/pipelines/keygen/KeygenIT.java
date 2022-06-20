package org.gbif.pipelines.keygen;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.junit.Test;

public class KeygenIT extends HBaseIT {

  @Test
  public void testNewOccurrenceTripletKey() {

    // State
    String occurrenceId = "occurrenceId";
    String triplet = "triplet";

    SimpleOccurrenceRecord occurrenceRecord = SimpleOccurrenceRecord.create();
    occurrenceRecord.setOccurrenceId(occurrenceId);
    occurrenceRecord.setTriplet(triplet);

    // When
    Optional<Long> key = Keygen.getKey(keyService, true, true, false, occurrenceRecord);

    // Should
    assertFalse(key.isPresent());
  }

  @Test
  public void testExistingOccurrenceTripletKey() {

    // State
    String occurrenceId = "occurrenceId";
    String triplet = "triplet";

    SimpleOccurrenceRecord occurrenceRecord = SimpleOccurrenceRecord.create();
    occurrenceRecord.setOccurrenceId(occurrenceId);
    occurrenceRecord.setTriplet(triplet);

    KeyLookupResult expected =
        keyService.generateKey(new HashSet<>(Arrays.asList(occurrenceId, triplet)));

    // When
    Optional<Long> key = Keygen.getKey(keyService, true, true, false, occurrenceRecord);

    // Should
    assertTrue(key.isPresent());
    assertEquals(Long.valueOf(expected.getKey()), key.get());
  }

  @Test
  public void testRelinkKeyUsingTriplet() {

    // State
    String occurrenceId = "occurrenceId";
    String triplet = "triplet";
    String newOccurrenceId = "newOccurrenceId";

    SimpleOccurrenceRecord occurrenceRecord = SimpleOccurrenceRecord.create();
    occurrenceRecord.setOccurrenceId(newOccurrenceId);
    occurrenceRecord.setTriplet(triplet);

    SimpleOccurrenceRecord occurrenceOnlyRecord = SimpleOccurrenceRecord.create();
    occurrenceOnlyRecord.setOccurrenceId(newOccurrenceId);

    KeyLookupResult expected =
        keyService.generateKey(new HashSet<>(Arrays.asList(occurrenceId, triplet)));

    // When
    Optional<Long> relinkKey = Keygen.getKey(keyService, true, true, false, occurrenceRecord);
    Optional<Long> occurrenceKey =
        Keygen.getKey(keyService, true, true, false, occurrenceOnlyRecord);

    // Should
    assertTrue(relinkKey.isPresent());
    assertTrue(occurrenceKey.isPresent());
    assertEquals(Long.valueOf(expected.getKey()), relinkKey.get());
    assertEquals(occurrenceKey.get(), relinkKey.get());
  }
}
