package org.gbif.pipelines.diagnostics.tools;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import org.gbif.pipelines.diagnostics.MainTool;
import org.gbif.pipelines.diagnostics.common.HbaseServer;
import org.gbif.pipelines.diagnostics.strategy.DeletionStrategy.DeletionStrategyType;
import org.gbif.pipelines.diagnostics.strategy.LookupKeyUtils;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class RepairGbifIDLookupToolIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test(expected = IllegalArgumentException.class)
  public void noSourcesTest() {

    // State
    String[] args = {
      "--dataset-key",
      "508089ca-ddb4-4112-b2cb-cb1bff8f39ad",
      "--lookup-table",
      HbaseServer.CFG.getLookupTable(),
      "--deletion-strategy",
      "BOTH",
      "--only-collisions",
      "--tool",
      "REPAIR"
    };

    // When
    MainTool.main(args);
  }

  @Test(expected = IllegalArgumentException.class)
  public void mixedSourcesTest() {

    // State
    String[] args = {
      "--input-source",
      this.getClass().getResource("/dwca").getFile(),
      "--dataset-key",
      "508089ca-ddb4-4112-b2cb-cb1bff8f39ad",
      "--triplet-lookup-key",
      "triplet",
      "--occurrenceID-lookup-key",
      "occId",
      "--lookup-table",
      HbaseServer.CFG.getLookupTable(),
      "--deletion-strategy",
      "BOTH",
      "--only-collisions"
    };

    // When
    MainTool.main(args);
  }

  @Test(expected = IllegalArgumentException.class)
  public void noStrategyFlagTest() {

    // State
    String[] args = {
      "--dataset-key",
      "508089ca-ddb4-4112-b2cb-cb1bff8f39ad",
      "--triplet-lookup-key",
      "triplet",
      "--occurrenceID-lookup-key",
      "occId",
      "--lookup-table",
      HbaseServer.CFG.getLookupTable(),
      "--only-collisions"
    };

    // When
    MainTool.main(args);
  }

  @Test(expected = IllegalArgumentException.class)
  public void noLookupTableFlagTest() {

    // State
    String[] args = {
      "--dataset-key",
      "508089ca-ddb4-4112-b2cb-cb1bff8f39ad",
      "--triplet-lookup-key",
      "triplet",
      "--occurrenceID-lookup-key",
      "occId",
      "--deletion-strategy",
      "BOTH",
      "--only-collisions"
    };

    // When
    MainTool.main(args);
  }

  @Test
  public void lookupKeysTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    keygenService.generateKey(new HashSet<>(Arrays.asList(occId, triplet)));

    // Should
    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertTrue(tripletKey.isPresent());
    Assert.assertTrue(occurrenceIdtKey.isPresent());

    // When
    RepairGbifIDLookupTool.builder()
        .datasetKey(datasetKey)
        .tripletLookupKey(triplet)
        .occurrenceIdLookupKey(occId)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .deletionStrategyType(DeletionStrategyType.BOTH)
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertFalse(tripletKey.isPresent());
    Assert.assertFalse(occurrenceIdtKey.isPresent());
  }

  @Test
  public void lookupKeysCollisionTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    keygenService.generateKey(new HashSet<>(Arrays.asList(occId, triplet)));

    // Should
    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertTrue(tripletKey.isPresent());
    Assert.assertTrue(occurrenceIdtKey.isPresent());

    // When
    RepairGbifIDLookupTool.builder()
        .datasetKey(datasetKey)
        .tripletLookupKey(triplet)
        .occurrenceIdLookupKey(occId)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .deletionStrategyType(DeletionStrategyType.BOTH)
        .connection(HBASE_SERVER.getConnection())
        .onlyCollisions(true)
        .build()
        .run();

    // Should
    tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertTrue(tripletKey.isPresent());
    Assert.assertTrue(occurrenceIdtKey.isPresent());
  }

  @Test
  public void dwcaTest() {

    // State
    File dwca = new File(this.getClass().getResource("/dwca/regular").getFile());

    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "926773";
    String triplet =
        OccurrenceKeyBuilder.buildKey(
                "AWI", "Kongsfjorden/Spitsbergen - soft bottom fauna", "MarBEF/MacroBEN_926773")
            .orElse(null);

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    keygenService.generateKey(new HashSet<>(Arrays.asList(occId, triplet)));

    // Should
    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertTrue(tripletKey.isPresent());
    Assert.assertTrue(occurrenceIdtKey.isPresent());

    // When
    RepairGbifIDLookupTool.builder()
        .datasetKey(datasetKey)
        .dwcaSource(dwca)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .deletionStrategyType(DeletionStrategyType.BOTH)
        .connection(HBASE_SERVER.getConnection())
        .build()
        .run();

    // Should
    tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertFalse(tripletKey.isPresent());
    Assert.assertFalse(occurrenceIdtKey.isPresent());
  }

  @Test
  public void dwcaDryRunTest() {

    // State
    File dwca = new File(this.getClass().getResource("/dwca/regular").getFile());

    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "926773";
    String triplet =
        OccurrenceKeyBuilder.buildKey(
                "AWI", "Kongsfjorden/Spitsbergen - soft bottom fauna", "MarBEF/MacroBEN_926773")
            .orElse(null);

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    keygenService.generateKey(new HashSet<>(Arrays.asList(occId, triplet)));

    // Should
    Optional<Long> tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    Optional<Long> occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertTrue(tripletKey.isPresent());
    Assert.assertTrue(occurrenceIdtKey.isPresent());

    // When
    RepairGbifIDLookupTool.builder()
        .datasetKey(datasetKey)
        .dwcaSource(dwca)
        .lookupTable(HbaseServer.CFG.getLookupTable())
        .counterTable(HbaseServer.CFG.getCounterTable())
        .occurrenceTable(HbaseServer.CFG.getOccurrenceTable())
        .deletionStrategyType(DeletionStrategyType.BOTH)
        .connection(HBASE_SERVER.getConnection())
        .dryRun(true)
        .build()
        .run();

    // Should
    tripletKey = LookupKeyUtils.getKey(keygenService, triplet);
    occurrenceIdtKey = LookupKeyUtils.getKey(keygenService, occId);

    Assert.assertTrue(tripletKey.isPresent());
    Assert.assertTrue(occurrenceIdtKey.isPresent());
  }
}
