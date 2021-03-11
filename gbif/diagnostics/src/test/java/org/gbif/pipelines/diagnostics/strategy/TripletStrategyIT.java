package org.gbif.pipelines.diagnostics.strategy;

import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.diagnostics.common.HbaseServer;
import org.gbif.pipelines.diagnostics.common.HbaseStore;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.hbase.HBaseStore;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TripletStrategyIT {

  public static final HBaseStore<String> LOOKUP_TABLE_STORE = HbaseServer.lookupTableStore;
  public static final Connection CONNECTION = HbaseServer.connection;

  @Before
  public void before() throws IOException {
    HbaseServer.truncateTable();
  }

  @Test
  public void tripletTest() {

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
        LOOKUP_TABLE_STORE,
        HbaseStore.KV.create(lookupOccId, gbifId),
        HbaseStore.KV.create(lookupTriplet, gbifId),
        HbaseStore.KV.create(occId2, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, CONNECTION, datasetKey);

    // When
    Set<String> keysToDelete =
        new TripletStrategy().getKeysToDelete(keygenService, false, triplet, occId);

    // Should
    Assert.assertEquals(1, keysToDelete.size());
    Assert.assertTrue(keysToDelete.contains(triplet));
  }

  @Test
  public void tripletEmptyTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, CONNECTION, datasetKey);

    // When
    Set<String> keysToDelete =
        new TripletStrategy().getKeysToDelete(keygenService, false, triplet, occId);

    // Should
    Assert.assertEquals(0, keysToDelete.size());
  }

  @Test
  public void tripletNoCollisionTest() {

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
        LOOKUP_TABLE_STORE,
        HbaseStore.KV.create(lookupOccId, gbifId),
        HbaseStore.KV.create(lookupTriplet, gbifId),
        HbaseStore.KV.create(occId2, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, CONNECTION, datasetKey);

    // When
    Set<String> keysToDelete =
        new TripletStrategy().getKeysToDelete(keygenService, true, triplet, occId);

    // Should
    Assert.assertEquals(0, keysToDelete.size());
  }

  @Test
  public void tripletCollisionTest() {

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
        LOOKUP_TABLE_STORE,
        HbaseStore.KV.create(lookupOccId, gbifId),
        HbaseStore.KV.create(lookupTriplet, gbifId2),
        HbaseStore.KV.create(occId2, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, CONNECTION, datasetKey);

    // When
    Set<String> keysToDelete =
        new TripletStrategy().getKeysToDelete(keygenService, true, triplet, occId);

    // Should
    Assert.assertEquals(1, keysToDelete.size());
    Assert.assertTrue(keysToDelete.contains(triplet));
  }
}
