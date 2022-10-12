package org.gbif.pipelines.diagnostics.strategy;

import java.io.IOException;
import java.util.Map;
import org.gbif.pipelines.diagnostics.resources.HbaseServer;
import org.gbif.pipelines.diagnostics.resources.HbaseStore;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.identifier.OccurrenceKeyBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class MinStrategyIT {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final HbaseServer HBASE_SERVER = HbaseServer.getInstance();

  @Before
  public void before() throws IOException {
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void minTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);
    long gbifId = 1L;
    long gbifId2 = 2L;

    String lookupOccId = datasetKey + "|" + occId;
    String lookupTriplet = datasetKey + "|" + triplet;

    HbaseStore.putRecords(
        HBASE_SERVER.getLookupTableStore(),
        HbaseStore.KV.create(lookupOccId, gbifId),
        HbaseStore.KV.create(lookupTriplet, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    // When
    Map<String, Long> keysToDelete =
        new MinStrategy().getKeysToDelete(keygenService, false, triplet, occId);

    // Should
    Assert.assertEquals(1, keysToDelete.size());
    Assert.assertEquals(Long.valueOf(gbifId), keysToDelete.get(occId));
  }

  @Test
  public void minEqualTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);
    long gbifId = 1L;

    String lookupOccId = datasetKey + "|" + occId;
    String lookupTriplet = datasetKey + "|" + triplet;

    HbaseStore.putRecords(
        HBASE_SERVER.getLookupTableStore(),
        HbaseStore.KV.create(lookupOccId, gbifId),
        HbaseStore.KV.create(lookupTriplet, gbifId));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    // When
    Map<String, Long> keysToDelete =
        new MinStrategy().getKeysToDelete(keygenService, false, triplet, occId);

    // Should
    Assert.assertEquals(0, keysToDelete.size());
  }

  @Test
  public void minInvertedTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);
    long gbifId = 1L;
    long gbifId2 = 2L;

    String lookupOccId = datasetKey + "|" + occId;
    String lookupTriplet = datasetKey + "|" + triplet;

    HbaseStore.putRecords(
        HBASE_SERVER.getLookupTableStore(),
        HbaseStore.KV.create(lookupOccId, gbifId2),
        HbaseStore.KV.create(lookupTriplet, gbifId));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    // When
    Map<String, Long> keysToDelete =
        new MinStrategy().getKeysToDelete(keygenService, false, triplet, occId);

    // Should
    Assert.assertEquals(1, keysToDelete.size());
    Assert.assertEquals(Long.valueOf(gbifId), keysToDelete.get(triplet));
  }

  @Test
  public void minEmptyTest() {

    // State
    String datasetKey = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";

    String occId = "5760d633-2efa-4359-bbbb-635f7c200803";
    String triplet = OccurrenceKeyBuilder.buildKey("ic", "cc", "cn").orElse(null);
    long gbifId2 = 2L;

    String lookupOccId = datasetKey + "|" + occId;

    HbaseStore.putRecords(
        HBASE_SERVER.getLookupTableStore(), HbaseStore.KV.create(lookupOccId, gbifId2));

    HBaseLockingKeyService keygenService =
        new HBaseLockingKeyService(HbaseServer.CFG, HBASE_SERVER.getConnection(), datasetKey);

    // When
    Map<String, Long> keysToDelete =
        new MinStrategy().getKeysToDelete(keygenService, false, triplet, occId);

    // Should
    Assert.assertEquals(0, keysToDelete.size());
  }
}
