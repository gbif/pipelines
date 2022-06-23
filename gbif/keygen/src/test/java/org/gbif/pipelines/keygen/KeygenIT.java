package org.gbif.pipelines.keygen;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class KeygenIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void testNewOccurrenceTripletKey() {

    // State
    String occurrenceId = "occurrenceId";
    String triplet = "triplet";

    SimpleOccurrenceRecord occurrenceRecord = SimpleOccurrenceRecord.create();
    occurrenceRecord.setOccurrenceId(occurrenceId);
    occurrenceRecord.setTriplet(triplet);

    // When
    Optional<Long> key =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceRecord);

    // Should
    assertFalse(key.isPresent());
  }

  @Test
  public void testGenerateOccurrenceTripletKey() {

    // State
    String occurrenceId = "occurrenceId";
    String triplet = "triplet";

    SimpleOccurrenceRecord occurrenceRecord = SimpleOccurrenceRecord.create();
    occurrenceRecord.setOccurrenceId(occurrenceId);
    occurrenceRecord.setTriplet(triplet);

    // When

    // Check that key is empty
    Optional<Long> nullKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceRecord);

    // Generate
    Optional<Long> newKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, true, occurrenceRecord);

    // Get key by occurrenceId
    SimpleOccurrenceRecord occurrenceOnlyRecord = SimpleOccurrenceRecord.create();
    occurrenceOnlyRecord.setOccurrenceId(occurrenceId);
    Optional<Long> occurrenceIdKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceOnlyRecord);

    // Get key by triplet
    SimpleOccurrenceRecord tripletOnlyRecord = SimpleOccurrenceRecord.create();
    tripletOnlyRecord.setTriplet(triplet);
    Optional<Long> tripletKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, tripletOnlyRecord);

    // Should
    assertFalse(nullKey.isPresent());
    assertFalse(tripletKey.isPresent());

    assertTrue(newKey.isPresent());
    assertTrue(occurrenceIdKey.isPresent());
    assertEquals(newKey.get(), occurrenceIdKey.get());
  }

  @Test
  public void testOccurrenceTripletChange() {

    // State
    String occurrenceId = "occurrenceId";
    String triplet = "triplet";
    String newTriplet = "newTriplet";

    // When

    // Generate key for triplet
    SimpleOccurrenceRecord tripletOnlyRecord = SimpleOccurrenceRecord.create();
    tripletOnlyRecord.setTriplet(triplet);
    Optional<Long> tripletKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, true, tripletOnlyRecord);

    // Relink occurrence to triplet key
    SimpleOccurrenceRecord occurrenceRecord = SimpleOccurrenceRecord.create();
    occurrenceRecord.setOccurrenceId(occurrenceId);
    occurrenceRecord.setTriplet(triplet);
    Optional<Long> relinkKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceRecord);

    // Get key by occurrenceId
    SimpleOccurrenceRecord occurrenceOnlyRecord = SimpleOccurrenceRecord.create();
    occurrenceOnlyRecord.setOccurrenceId(occurrenceId);
    Optional<Long> occurrenceIdKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceOnlyRecord);

    // The key is the same because it linked to occurrenceId
    SimpleOccurrenceRecord newOccurrenceRecord = SimpleOccurrenceRecord.create();
    newOccurrenceRecord.setOccurrenceId(occurrenceId);
    newOccurrenceRecord.setTriplet(newTriplet);
    Optional<Long> sameOccurrenceIdKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, newOccurrenceRecord);

    // Use only triplet to check the key
    SimpleOccurrenceRecord newTripletRecord = SimpleOccurrenceRecord.create();
    newTripletRecord.setTriplet(newTriplet);
    Optional<Long> newTripletKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, newTripletRecord);

    // Should
    assertFalse(newTripletKey.isPresent());

    assertTrue(tripletKey.isPresent());
    assertTrue(relinkKey.isPresent());
    assertTrue(occurrenceIdKey.isPresent());
    assertTrue(sameOccurrenceIdKey.isPresent());

    assertEquals(tripletKey.get(), relinkKey.get());
    assertEquals(relinkKey.get(), occurrenceIdKey.get());
    assertEquals(occurrenceIdKey.get(), sameOccurrenceIdKey.get());
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
        HBASE_SERVER.keyService.generateKey(new HashSet<>(Arrays.asList(occurrenceId, triplet)));

    // When
    Optional<Long> key =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceRecord);

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
        HBASE_SERVER.keyService.generateKey(new HashSet<>(Arrays.asList(occurrenceId, triplet)));

    // When
    Optional<Long> relinkKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceRecord);
    Optional<Long> occurrenceKey =
        Keygen.getKey(HBASE_SERVER.keyService, true, true, false, occurrenceOnlyRecord);

    // Should
    assertTrue(relinkKey.isPresent());
    assertTrue(occurrenceKey.isPresent());
    assertEquals(Long.valueOf(expected.getKey()), relinkKey.get());
    assertEquals(occurrenceKey.get(), relinkKey.get());
  }

  @Test
  public void testNewTripletKeyOnly() {

    // State
    String triplet = "triplet";

    SimpleOccurrenceRecord occurrenceRecord = SimpleOccurrenceRecord.create();
    occurrenceRecord.setTriplet(triplet);

    // When
    Optional<Long> key =
        Keygen.getKey(HBASE_SERVER.keyService, true, false, true, occurrenceRecord);

    // Should
    assertTrue(key.isPresent());
  }
}
