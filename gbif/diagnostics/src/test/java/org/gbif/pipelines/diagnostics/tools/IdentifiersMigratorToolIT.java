package org.gbif.pipelines.diagnostics.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.gbif.pipelines.diagnostics.common.HbaseServer;
import org.gbif.pipelines.keygen.api.KeyLookupResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class IdentifiersMigratorToolIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void testNoExistingKeys() {

    // State
    String file = this.getClass().getResource("/migration.csv").getFile();
    String datasetKey = UUID.randomUUID().toString();
    String old1Occurrence = "old1";
    String old2Occurrence = "old2";
    String new1Occurrence = "new1";
    String new2Occurrence = "new2";

    // When
    IdentifiersMigratorTool.builder()
        .toDatasetKey(datasetKey)
        .fromDatasetKey(datasetKey)
        .filePath(file)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    Optional<KeyLookupResult> old1Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old1Occurrence), datasetKey);
    Optional<KeyLookupResult> old2Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old2Occurrence), datasetKey);
    Optional<KeyLookupResult> new1Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new1Occurrence), datasetKey);
    Optional<KeyLookupResult> new2Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new2Occurrence), datasetKey);

    Assert.assertFalse(old1Key.isPresent());
    Assert.assertFalse(old2Key.isPresent());
    Assert.assertFalse(new1Key.isPresent());
    Assert.assertFalse(new2Key.isPresent());
  }

  @Test
  public void testFileIssue() {

    // State
    String file = this.getClass().getResource("/migration-file-issue.csv").getFile();
    String datasetKey = UUID.randomUUID().toString();
    String old1Occurrence = "old1";
    String old2Occurrence = "old2";
    String new1Occurrence = "new1";
    String new2Occurrence = "new2";

    // When
    IdentifiersMigratorTool.builder()
        .deleteKeys(true)
        .toDatasetKey(datasetKey)
        .fromDatasetKey(datasetKey)
        .filePath(file)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    Optional<KeyLookupResult> old1Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old1Occurrence), datasetKey);
    Optional<KeyLookupResult> old2Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old2Occurrence), datasetKey);
    Optional<KeyLookupResult> new1Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new1Occurrence), datasetKey);
    Optional<KeyLookupResult> new2Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new2Occurrence), datasetKey);

    Assert.assertFalse(old1Key.isPresent());
    Assert.assertFalse(old2Key.isPresent());
    Assert.assertFalse(new1Key.isPresent());
    Assert.assertFalse(new2Key.isPresent());
  }

  @Test
  public void testMigrationWithDeletion() {

    // State
    String file = this.getClass().getResource("/migration.csv").getFile();
    String datasetKey = UUID.randomUUID().toString();
    String old1Occurrence = "old1";
    String old2Occurrence = "old2";
    String new1Occurrence = "new1";
    String new2Occurrence = "new2";

    KeyLookupResult old1Key =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old1Occurrence), datasetKey);
    KeyLookupResult old2Key =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old2Occurrence), datasetKey);

    // When
    IdentifiersMigratorTool.builder()
        .deleteKeys(true)
        .toDatasetKey(datasetKey)
        .fromDatasetKey(datasetKey)
        .filePath(file)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    Optional<KeyLookupResult> old1DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old1Occurrence), datasetKey);
    Optional<KeyLookupResult> old2DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old2Occurrence), datasetKey);
    Optional<KeyLookupResult> new1Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new1Occurrence), datasetKey);
    Optional<KeyLookupResult> new2Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new2Occurrence), datasetKey);

    Assert.assertFalse(old1DeletedKey.isPresent());
    Assert.assertFalse(old2DeletedKey.isPresent());
    Assert.assertTrue(new1Key.isPresent());
    Assert.assertTrue(new2Key.isPresent());
    Assert.assertEquals(old1Key.getKey(), new1Key.get().getKey());
    Assert.assertEquals(old2Key.getKey(), new2Key.get().getKey());
  }

  @Test
  public void testDatasetMigrationWithDeletion() {

    // State
    String file = this.getClass().getResource("/migration.csv").getFile();
    String datasetKey = UUID.randomUUID().toString();
    String newDatasetKey = UUID.randomUUID().toString();
    String old1Occurrence = "old1";
    String old2Occurrence = "old2";
    String new1Occurrence = "new1";
    String new2Occurrence = "new2";

    KeyLookupResult old1Key =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old1Occurrence), datasetKey);
    KeyLookupResult old2Key =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old2Occurrence), datasetKey);

    // When
    IdentifiersMigratorTool.builder()
        .deleteKeys(true)
        .fromDatasetKey(datasetKey)
        .toDatasetKey(newDatasetKey)
        .filePath(file)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    Optional<KeyLookupResult> old1DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old1Occurrence), datasetKey);
    Optional<KeyLookupResult> old2DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old2Occurrence), datasetKey);
    Optional<KeyLookupResult> new1Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new1Occurrence), newDatasetKey);
    Optional<KeyLookupResult> new2Key =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new2Occurrence), newDatasetKey);

    Assert.assertFalse(old1DeletedKey.isPresent());
    Assert.assertFalse(old2DeletedKey.isPresent());
    Assert.assertTrue(new1Key.isPresent());
    Assert.assertTrue(new2Key.isPresent());
    Assert.assertEquals(old1Key.getKey(), new1Key.get().getKey());
    Assert.assertEquals(old2Key.getKey(), new2Key.get().getKey());
  }

  @Test
  public void testMigrationIssuesWithoutDeletion() {
    // State
    String file = this.getClass().getResource("/migration.csv").getFile();
    String datasetKey = UUID.randomUUID().toString();
    String newDatasetKey = UUID.randomUUID().toString();
    String old1Occurrence = "old1";
    String old2Occurrence = "old2";
    String new1Occurrence = "new1";
    String new2Occurrence = "new2";

    KeyLookupResult old1ExistingKey =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old1Occurrence), datasetKey);
    KeyLookupResult old2ExistingKey =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old2Occurrence), datasetKey);
    KeyLookupResult new1ExistingKey =
        HBASE_SERVER
            .getKeyService()
            .generateKey(Collections.singleton(new1Occurrence), newDatasetKey);
    KeyLookupResult new2ExistingKey =
        HBASE_SERVER
            .getKeyService()
            .generateKey(Collections.singleton(new2Occurrence), newDatasetKey);

    // When
    IdentifiersMigratorTool.builder()
        .deleteKeys(false)
        .skipIssues(true)
        .fromDatasetKey(datasetKey)
        .toDatasetKey(newDatasetKey)
        .filePath(file)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    Optional<KeyLookupResult> old1DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old1Occurrence), datasetKey);
    Optional<KeyLookupResult> old2DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old2Occurrence), datasetKey);
    Optional<KeyLookupResult> new1MigratedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new1Occurrence), newDatasetKey);
    Optional<KeyLookupResult> new2MigratedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new2Occurrence), newDatasetKey);

    Assert.assertTrue(old1DeletedKey.isPresent());
    Assert.assertTrue(old2DeletedKey.isPresent());
    Assert.assertTrue(new1MigratedKey.isPresent());
    Assert.assertTrue(new2MigratedKey.isPresent());
    Assert.assertEquals(old1ExistingKey.getKey(), old1DeletedKey.get().getKey());
    Assert.assertEquals(old2ExistingKey.getKey(), old2DeletedKey.get().getKey());
    Assert.assertEquals(new1ExistingKey.getKey(), new1MigratedKey.get().getKey());
    Assert.assertEquals(new2ExistingKey.getKey(), new2MigratedKey.get().getKey());
  }

  @Test
  public void testMigrationIssuesWithDeletion() {
    // State
    String file = this.getClass().getResource("/migration.csv").getFile();
    String datasetKey = UUID.randomUUID().toString();
    String newDatasetKey = UUID.randomUUID().toString();
    String old1Occurrence = "old1";
    String old2Occurrence = "old2";
    String new1Occurrence = "new1";
    String new2Occurrence = "new2";

    KeyLookupResult old1ExistingKey =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old1Occurrence), datasetKey);
    KeyLookupResult old2ExistingKey =
        HBASE_SERVER.getKeyService().generateKey(Collections.singleton(old2Occurrence), datasetKey);
    HBASE_SERVER.getKeyService().generateKey(Collections.singleton(new1Occurrence), newDatasetKey);
    HBASE_SERVER.getKeyService().generateKey(Collections.singleton(new2Occurrence), newDatasetKey);

    // When
    IdentifiersMigratorTool.builder()
        .deleteKeys(true)
        .skipIssues(true)
        .fromDatasetKey(datasetKey)
        .toDatasetKey(newDatasetKey)
        .filePath(file)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    Optional<KeyLookupResult> old1DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old1Occurrence), datasetKey);
    Optional<KeyLookupResult> old2DeletedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(old2Occurrence), datasetKey);
    Optional<KeyLookupResult> new1MigratedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new1Occurrence), newDatasetKey);
    Optional<KeyLookupResult> new2MigratedKey =
        HBASE_SERVER.getKeyService().findKey(Collections.singleton(new2Occurrence), newDatasetKey);

    Assert.assertFalse(old1DeletedKey.isPresent());
    Assert.assertFalse(old2DeletedKey.isPresent());
    Assert.assertTrue(new1MigratedKey.isPresent());
    Assert.assertTrue(new2MigratedKey.isPresent());
    Assert.assertEquals(old1ExistingKey.getKey(), new1MigratedKey.get().getKey());
    Assert.assertEquals(old2ExistingKey.getKey(), new2MigratedKey.get().getKey());
  }
}
