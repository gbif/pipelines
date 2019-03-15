package org.gbif.pipeleins.keygen;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.gbif.hbase.util.ResultReader;
import org.gbif.pipeleins.api.KeyLookupResult;
import org.gbif.pipeleins.config.OccHBaseConfiguration;
import org.gbif.pipeleins.hbase.Columns;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

/**
 * An extension of AbstractHBaseKeyPersistenceService with a generateKey implementation that uses an HBase
 * implementation of the algorithm described at
 * <a href="http://dev.gbif.org/code/snippet/CR-OCC-5">http://dev.gbif.org/code/snippet/CR-OCC-5</a>.
 */
@Slf4j
public class HBaseLockingKeyService extends AbstractHBaseKeyPersistenceService {

  private static final long WAIT_BEFORE_RETRY_MS = 5000;
  private static final int WAIT_SKEW = 4000;
  private static final long STALE_LOCK_TIME = 5 * 60 * 1000;
  public static final int COUNTER_ROW = 1;

  // The number of IDs to reserve at a time in batch
  private static final Integer BATCHED_ID_SIZE = 100;
  // the next available key to allocate
  private int currentKey;
  // our reserved upper key limit for the current batch
  private int maxReservedKeyInclusive;

  public HBaseLockingKeyService(OccHBaseConfiguration cfg, Connection connection) {
    super(cfg, connection, new OccurrenceKeyBuilder());
  }

  @Override
  public KeyLookupResult generateKey(Set<String> uniqueStrings, String scope) {
    Map<String, KeyStatus> statusMap = Maps.newTreeMap(); // required: predictable sorting for e.g. testing
    Map<String, Integer> existingKeyMap = Maps.newTreeMap(); // required: predictable sorting for e.g. testing
    byte[] lockId = Bytes.toBytes(UUID.randomUUID().toString());

    // lookupTable schema: lookupKey | status | lock | key

    // all of our locks will have the same timestamp
    long now = System.currentTimeMillis();

    Set<String> lookupKeys = keyBuilder.buildKeys(uniqueStrings, scope);
    boolean failed = false;
    Integer key = null;
    Integer foundKey = null;
    for (String lookupKey : lookupKeys) {
      Result row = lookupTableStore.getRow(lookupKey);
      log.debug("Lookup for [{}] produced [{}]", lookupKey, row);
      KeyStatus status = null;
      byte[] existingLock = null;
      if (row != null) {
        String rawStatus = ResultReader.getString(row, Columns.OCCURRENCE_COLUMN_FAMILY,
                                                  Columns.LOOKUP_STATUS_COLUMN, null);
        if (rawStatus != null) {
          status = KeyStatus.valueOf(rawStatus);
        }
        existingLock = ResultReader.getBytes(row, Columns.OCCURRENCE_COLUMN_FAMILY,
                                             Columns.LOOKUP_LOCK_COLUMN, null);
        key = ResultReader.getInteger(row, Columns.OCCURRENCE_COLUMN_FAMILY,
                                      Columns.LOOKUP_KEY_COLUMN, null);
        log.debug("Got existing status [{}] existingLock [{}] key [{}]", status, existingLock, key);
      }

      if (status == KeyStatus.ALLOCATED) {
        log.debug("Status ALLOCATED, using found key [{}]", key);
        // even if existingLock is != null, ALLOCATED means the key exists and is final
        statusMap.put(lookupKey, KeyStatus.ALLOCATED);
        existingKeyMap.put(lookupKey, key);
        if (foundKey == null) {
          foundKey = key;
        } else {
          // we've found conflicting keys for our lookupKeys - this is fatal
          if (foundKey.intValue() != key.intValue()) {
            failWithConflictingLookup(existingKeyMap);
          }
        }
        log.debug("Status ALLOCATED, using found key [{}]", foundKey);
      } else if (existingLock == null) {
        // lock is ours for the taking - checkAndPut lockId, expecting null for lockId
        boolean gotLock = lookupTableStore.checkAndPut(lookupKey, Columns.LOOKUP_LOCK_COLUMN, lockId,
                                                       Columns.LOOKUP_LOCK_COLUMN, null, now);
        if (gotLock) {
          statusMap.put(lookupKey, KeyStatus.ALLOCATING);
          log.debug("Grabbed free lock, now ALLOCATING [{}]", lookupKey);
        } else {
          failed = true;
          log.debug("Failed to grab free lock for [{}], breaking", lookupKey);
          break;
        }
      } else {
        // somebody has written their lockId and so has the lock, but they haven't finished yet (status != ALLOCATED)
        Long existingLockTs = ResultReader.getTimestamp(row, Columns.OCCURRENCE_COLUMN_FAMILY,
                                                        Columns.LOOKUP_LOCK_COLUMN);
        if (now - existingLockTs > STALE_LOCK_TIME) {
          log.debug("Found stale lock for [{}]", lookupKey);
          // Someone died before releasing lock.
          // Note that key could be not null here - this means that thread had the lock, wrote the key, but then
          // died before releasing lock.
          // checkandPut our lockId, expecting lock to match the existing lock
          boolean gotLock = lookupTableStore.checkAndPut(lookupKey, Columns.LOOKUP_LOCK_COLUMN,
                                                         lockId, Columns.LOOKUP_LOCK_COLUMN, existingLock, now);
          if (gotLock) {
            statusMap.put(lookupKey, KeyStatus.ALLOCATING);
            log.debug("Reset stale lock, now ALLOCATING [{}]", lookupKey);
          } else {
            // someone beat us to this lock, in one of two ways
            // 1) they grabbed lock, wrote new id, and released lock, so now status is ALLOCATED and id is final
            // 2) they grabbed lock so status is a newer lock uuid with recent timestamp
            // in either case we're toast - abort and try again
            failed = true;
            log.debug("Failed to reset stale lock for [{}], breaking", lookupKey);
            break;
          }
        } else {
          // someone has a current lock, we need to give up and try again
          failed = true;
          log.debug("Hit valid, current lock for [{}], breaking", lookupKey);
          break;
        }
      }
    }

    if (failed) {
      log.debug("Failed to get lock. Releasing held locks and trying again.");
      releaseLocks(statusMap);
      try {
        Random random = new Random();
        TimeUnit.MILLISECONDS.sleep(WAIT_BEFORE_RETRY_MS + random.nextInt(WAIT_SKEW) - random.nextInt(WAIT_SKEW));
      } catch (InterruptedException e) {
        // do nothing
      }
      // recurse
      return generateKey(uniqueStrings, scope);
    }

    // now we have map of every lookupKey to either ALLOCATED or ALLOCATING, and locks on all ALLOCATING
    KeyLookupResult lookupResult;
    if (foundKey == null) {
      key = getNextKey();
      lookupResult = new KeyLookupResult(key, true);
      log.debug("Now assigning new key [{}]", key);
    } else {
      key = foundKey;
      lookupResult = new KeyLookupResult(key, false);
      log.debug("Using found key [{}]", key);
    }

    // write the key and update status to ALLOCATED
    for (Map.Entry<String, KeyStatus> entry : statusMap.entrySet()) {
      if (entry.getValue() == KeyStatus.ALLOCATING) {
        // TODO: combine into one put
        lookupTableStore.putInt(entry.getKey(), Columns.LOOKUP_KEY_COLUMN, key);
        lookupTableStore.putString(entry.getKey(), Columns.LOOKUP_STATUS_COLUMN, KeyStatus.ALLOCATED.toString());
      }
    }

    releaseLocks(statusMap);

    log.debug("<< generateKey (generated? [{}] key [{}])", !key.equals(foundKey), key);

    return lookupResult;

  }

  /**
   * Provides the next available key. Because throughput of an incrementColumnValue is limited by HBase to a few
   * thousand calls per second, this implementation reserves a batch of IDs at a time, and then allocates them to
   * the calling threads, until they are exhausted, when it will go and reserve another batch. Failure scenarios
   * will therefore mean IDs go unused. This is expected to be a rare scenario and therefore acceptable.
   *
   * @return the next key
   */
  private synchronized int getNextKey() {
    // if we have exhausted our reserved keys, get a new batch of them
    if (currentKey == maxReservedKeyInclusive) {
      // get batch
      Long longKey = counterTableStore.incrementColumnValue(COUNTER_ROW, Columns.COUNTER_COLUMN, BATCHED_ID_SIZE.longValue());
      if (longKey > Integer.MAX_VALUE) {
        throw new IllegalStateException("HBase issuing keys larger than Integer can support");
      }
      maxReservedKeyInclusive = longKey.intValue();
      // safer to calculate our guaranteed safe range than rely on what nextKey was set to
      currentKey = maxReservedKeyInclusive - BATCHED_ID_SIZE;
    }
    currentKey++;
    return currentKey;
  }

  private void releaseLocks(Map<String, KeyStatus> statusMap) {
    for (Map.Entry<String, KeyStatus> entry : statusMap.entrySet()) {
      if (entry.getValue() == KeyStatus.ALLOCATING) {
        lookupTableStore.delete(entry.getKey(), Columns.LOOKUP_LOCK_COLUMN);
      }
    }
  }

  private enum KeyStatus {
    ALLOCATING, ALLOCATED
  }
}


