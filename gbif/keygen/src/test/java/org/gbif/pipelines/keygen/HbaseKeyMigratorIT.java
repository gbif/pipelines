package org.gbif.pipelines.keygen;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class HbaseKeyMigratorIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void testLookupKeyMigration() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    KeyLookupResult oldKey =
        HBASE_SERVER.keyService.generateKey(Collections.singleton(oldOccurrenceId), datasetKey);

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(HBASE_SERVER.keyService)
            .deleteKeys(false)
            .build()
            .migrate();

    // Should
    Optional<KeyLookupResult> newKey =
        HBASE_SERVER.keyService.findKey(Collections.singleton(newOccurrenceId), datasetKey);
    Optional<KeyLookupResult> oldExpiriedKey =
        HBASE_SERVER.keyService.findKey(Collections.singleton(oldOccurrenceId), datasetKey);

    assertTrue(migratedKey.isPresent());
    assertEquals(oldKey.getKey(), migratedKey.get().getKey());
    assertTrue(newKey.isPresent());
    assertEquals(oldKey.getKey(), newKey.get().getKey());
    assertFalse(oldExpiriedKey.isPresent());
  }

  @Test
  public void testNullLookupKey() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(HBASE_SERVER.keyService)
            .build()
            .migrate();

    // Should
    assertFalse(migratedKey.isPresent());
  }

  @Test
  public void testLookupKeyExisting() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    HBASE_SERVER.keyService.generateKey(Collections.singleton(oldOccurrenceId), datasetKey);
    HBASE_SERVER.keyService.generateKey(Collections.singleton(newOccurrenceId), datasetKey);

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(HBASE_SERVER.keyService)
            .deleteKeys(false)
            .build()
            .migrate();

    // Should
    assertFalse(migratedKey.isPresent());
  }

  @Test
  public void testLookupKeyDeleteExisting() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String oldOccurrenceId = "oldOccurrenceId";
    String newOccurrenceId = "newOccurrenceId";

    KeyLookupResult oldKey =
        HBASE_SERVER.keyService.generateKey(Collections.singleton(oldOccurrenceId), datasetKey);
    HBASE_SERVER.keyService.generateKey(Collections.singleton(newOccurrenceId), datasetKey);

    // When
    Optional<KeyLookupResult> migratedKey =
        HbaseKeyMigrator.builder()
            .fromDatasetKey(datasetKey)
            .toDatasetKey(datasetKey)
            .oldLookupKey(oldOccurrenceId)
            .newLookupKey(newOccurrenceId)
            .keyService(HBASE_SERVER.keyService)
            .deleteKeys(true)
            .build()
            .migrate();

    // Should
    Optional<KeyLookupResult> newKey =
        HBASE_SERVER.keyService.findKey(Collections.singleton(newOccurrenceId), datasetKey);
    Optional<KeyLookupResult> oldExpiriedKey =
        HBASE_SERVER.keyService.findKey(Collections.singleton(oldOccurrenceId), datasetKey);

    assertTrue(migratedKey.isPresent());
    assertEquals(oldKey.getKey(), migratedKey.get().getKey());
    assertTrue(newKey.isPresent());
    assertEquals(oldKey.getKey(), newKey.get().getKey());
    assertFalse(oldExpiriedKey.isPresent());
  }
}
