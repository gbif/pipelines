package org.gbif.pipeleins.keygen;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.identifier.OccurrenceKeyHelper;
import org.gbif.occurrence.persistence.IllegalDataStateException;
import org.gbif.occurrence.persistence.api.KeyLookupResult;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.HBaseStore;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract implementation of KeyPersistenceService that handles the finding and deleting of keys in HBase, but
 * leaves the generation of keys to sub-classes.
 */
public abstract class AbstractHBaseKeyPersistenceService implements KeyPersistenceService<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseKeyPersistenceService.class);
  private static final int HBASE_CLIENT_CACHING = 200;

  private final Connection connection;
  private final TableName lookupTableName;
  private final HBaseStore<Integer> occurrenceTableStore;
  protected final HBaseStore<String> lookupTableStore;
  protected final HBaseStore<Integer> counterTableStore;
  protected final KeyBuilder keyBuilder;

  public AbstractHBaseKeyPersistenceService(OccHBaseConfiguration cfg, Connection connection, KeyBuilder keyBuilder) {
    lookupTableName =  TableName.valueOf(checkNotNull(cfg.lookupTable, "lookupTable can't be null"));
    this.connection = checkNotNull(connection, "tablePool can't be null");
    this.keyBuilder = checkNotNull(keyBuilder, "keyBuilder can't be null");
    lookupTableStore = new HBaseStore<String>(cfg.lookupTable, Columns.OCCURRENCE_COLUMN_FAMILY, connection);
    counterTableStore = new HBaseStore<Integer>(cfg.counterTable, Columns.OCCURRENCE_COLUMN_FAMILY, connection);
    occurrenceTableStore = new HBaseStore<Integer>(cfg.occTable, Columns.OCCURRENCE_COLUMN_FAMILY, connection);
  }

  @Override
  public abstract KeyLookupResult generateKey(Set<String> uniqueStrings, String scope);

  @Override
  public KeyLookupResult findKey(Set<String> uniqueStrings, String scope) {
    checkNotNull(uniqueStrings, "uniqueStrings can't be null");
    checkNotNull(scope, "scope can't be null");
    if (uniqueStrings.isEmpty()) {
      return null;
    }

    Set<String> lookupKeys = keyBuilder.buildKeys(uniqueStrings, scope);
    Map<String, Integer> foundOccurrenceKeys = Maps.newTreeMap(); // required: predictable sorting for e.g. testing

    // get the occurrenceKey for each lookupKey, and set a flag if we find any null
    boolean gotNulls = false;
    for (String uniqueString : lookupKeys) {
      Integer occurrenceKey = lookupTableStore.getInt(uniqueString, Columns.LOOKUP_KEY_COLUMN);
      if (occurrenceKey == null) {
        gotNulls = true;
      } else {
        foundOccurrenceKeys.put(uniqueString, occurrenceKey);
      }
    }

    // go through all the returned keys and make sure they're all the same - if not, fail loudly (this means
    // an inconsistency in the db that we can't resolve here)
    Integer resultKey = null;
    for (String uniqueString : lookupKeys) {
      Integer occurrenceKey = foundOccurrenceKeys.get(uniqueString);
      if (occurrenceKey != null) {
        if (resultKey == null) {
          resultKey = occurrenceKey;
        } else if (resultKey.intValue() != occurrenceKey.intValue()) {
          failWithConflictingLookup(foundOccurrenceKeys);
        }
      }
    }

    // if we got an occurrenceKey as well as nulls, then we need to fill in the lookup table with the missing entries
    if (resultKey != null && gotNulls) {
      fillMissingKeys(lookupKeys, foundOccurrenceKeys, resultKey);
    }

    KeyLookupResult result = null;
    if (resultKey != null) {
      result = new KeyLookupResult(resultKey, false);
    }

    return result;
  }

  @Override
  public Set<Integer> findKeysByScope(String scope) {
    Set<Integer> keys = Sets.newHashSet();
    // note HTableStore isn't capable of ad hoc scans
    try (Table table = connection.getTable(lookupTableName)) {
      Scan scan = new Scan();
      scan.setCacheBlocks(false);
      scan.setCaching(HBASE_CLIENT_CACHING);
      scan.setFilter(new PrefixFilter(Bytes.toBytes(scope)));
      ResultScanner results = table.getScanner(scan);
      for (Result result : results) {
        byte[] rawKey = result.getValue(Columns.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN));
        if (rawKey != null) {
          keys.add(Bytes.toInt(rawKey));
        }
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    }
    return keys;
  }


  /**
   * Scans the lookup table for instances of the occurrenceKey and deletes those rows. It attempts to scope the scan
   * for this occurrenceKey within the dataset of the original occurrence, but note that there is no guarantee that the
   * original occurrence corresponding to this occurrenceKey still exists, so in the worst case this method will do a
   * full table scan of the lookup table.
   *
   * @param occurrenceKey the key to delete
   * @param datasetKey    the optional "scope" for the lookup (without it this method is very slow)
   */
  @Override
  public void deleteKey(Integer occurrenceKey, @Nullable String datasetKey) {
    checkNotNull(occurrenceKey, "occurrenceKey can't be null");

    // get the dataset for this occurrence if not handed in as scope
    String rawDatasetKey = datasetKey;
    if (rawDatasetKey == null) {
      rawDatasetKey = occurrenceTableStore.getString(occurrenceKey, Columns.column(GbifTerm.datasetKey));
    }

    // scan the lookup table for all rows where the key matches our dataset prefix and the cell value is our
    // target occurrenceKey, then delete those rows
    Scan scan = new Scan();
    scan.addColumn(Columns.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN));
    // TODO: this is still too slow even with prefix - lease timeouts in logs
    List<Filter> filters = Lists.newArrayList();
    if (rawDatasetKey == null) {
      LOG.warn("About to scan lookup table with no datasetKey prefix - target key for deletion is [{}]", occurrenceKey);
    } else {
      filters.add(new PrefixFilter(Bytes.toBytes(OccurrenceKeyHelper.buildKeyPrefix(rawDatasetKey))));
    }
    Filter valueFilter = new SingleColumnValueFilter(Columns.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN),
                                                     CompareFilter.CompareOp.EQUAL, Bytes.toBytes(occurrenceKey));
    filters.add(valueFilter);
    Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    scan.setFilter(filterList);
    try (Table lookupTable = connection.getTable(lookupTableName);
         ResultScanner resultScanner = lookupTable.getScanner(scan)) {
      List<Delete> keysToDelete = Lists.newArrayList();
      for (Result result : resultScanner) {
        Delete delete = new Delete(result.getRow());
        keysToDelete.add(delete);
      }
      if (!keysToDelete.isEmpty()) {
        lookupTable.delete(keysToDelete);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Failure accessing HBase", e);
    }

  }

  @Override
  public void deleteKeyByUniques(Set<String> uniqueStrings, String scope) {
    checkNotNull(uniqueStrings, "uniqueStrings can't be null");
    checkNotNull(scope, "scope can't be null");

    // craft a delete for every uniqueString
    Set<String> lookupKeys = keyBuilder.buildKeys(uniqueStrings, scope);
    List<Delete> keysToDelete = Lists.newArrayListWithCapacity(lookupKeys.size());
    for (String lookupKey : lookupKeys) {
      keysToDelete.add(new Delete(Bytes.toBytes(lookupKey)));
    }
    try (Table lookupTable = connection.getTable(lookupTableName)) {
      if (!keysToDelete.isEmpty()) {
        lookupTable.delete(keysToDelete);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Failure accessing HBase", e);
    }
  }

  protected static void failWithConflictingLookup(Map<String, Integer> conflictingKeys) {
    StringBuilder sb = new StringBuilder("Found inconsistent occurrence keys in looking up unique identifiers:");
    for (Map.Entry<String, Integer> entry : conflictingKeys.entrySet()) {
      sb.append('[').append(entry.getKey()).append("]=[").append(entry.getValue()).append(']');
    }
    throw new IllegalDataStateException(sb.toString());
  }


  private void fillMissingKeys(Set<String> lookupKeys, Map<String, Integer> foundOccurrenceKeys,
                               Integer occurrenceKey) {
    for (String lookupKey : lookupKeys) {
      if (!foundOccurrenceKeys.containsKey(lookupKey)) {
        lookupTableStore.putInt(lookupKey, Columns.LOOKUP_KEY_COLUMN, occurrenceKey);
      }
    }
  }
}
