package org.gbif.pipelines.diagnostics.strategy;

import org.gbif.pipelines.diagnostics.common.HbaseServer;
import org.gbif.pipelines.diagnostics.common.HbaseStore;
import org.gbif.pipelines.diagnostics.common.HbaseStore.KV;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class AllStrategyIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void allTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);
    long gbifId = 1L;

    String lookupOccId = datasetKey + "|" + occId;
    String lookupTriplet = datasetKey + "|" + triplet;

    String occId2 = "5760d633-2efa-4359-bbbb-635f7c200804";
    long gbifId2 = 2L;

    HbaseStore.putRecords(
        HBASE_SERVER.getLookupTableStore(),
        KV.create(lookupOccId, gbifId),
        KV.create(lookupTriplet, gbifId),
        KV.create(occId2, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    // When
    Set<String> keysToDelete =
        new AllStrategy().getKeysToDelete(keygenService, false, triplet, occId);

    // Should
    Assert.assertEquals(2, keysToDelete.size());
    Assert.assertTrue(keysToDelete.contains(occId));
    Assert.assertTrue(keysToDelete.contains(triplet));
  }

  @Test
  public void allNoCollisionTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);
    long gbifId = 1L;

    String lookupOccId = datasetKey + "|" + occId;
    String lookupTriplet = datasetKey + "|" + triplet;

    String occId2 = "5760d633-2efa-4359-bbbb-635f7c200804";
    long gbifId2 = 2L;

    HbaseStore.putRecords(
        HBASE_SERVER.getLookupTableStore(),
        KV.create(lookupOccId, gbifId),
        KV.create(lookupTriplet, gbifId),
        KV.create(occId2, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    // When
    Set<String> keysToDelete =
        new AllStrategy().getKeysToDelete(keygenService, true, triplet, occId);

    // Should
    Assert.assertEquals(0, keysToDelete.size());
  }

  @Test
  public void allCollisionTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);
    long gbifId = 1L;

    String lookupOccId = datasetKey + "|" + occId;
    String lookupTriplet = datasetKey + "|" + triplet;

    String occId2 = "5760d633-2efa-4359-bbbb-635f7c200804";
    long gbifId2 = 2L;

    HbaseStore.putRecords(
        HBASE_SERVER.getLookupTableStore(),
        KV.create(lookupOccId, gbifId),
        KV.create(lookupTriplet, gbifId2),
        KV.create(occId2, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    // When
    Set<String> keysToDelete =
        new AllStrategy().getKeysToDelete(keygenService, true, triplet, occId);

    // Should
    Assert.assertEquals(2, keysToDelete.size());
    Assert.assertTrue(keysToDelete.contains(occId));
    Assert.assertTrue(keysToDelete.contains(triplet));
  }
}
