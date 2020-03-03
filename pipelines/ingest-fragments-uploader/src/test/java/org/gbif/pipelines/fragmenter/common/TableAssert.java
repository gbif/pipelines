package org.gbif.pipelines.fragmenter.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.fragmenter.common.HbaseStore.getAttemptQualifier;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getDatasetIdQualifier;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getFragmentFamily;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getRecordQualifier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableAssert {

  public static void assertTableData(Connection connection, int expectedSize, String expectedDatasetId,
      Integer expectedAttempt) throws IOException {
    TableName tableName = TableName.valueOf(HbaseServer.FRAGMENT_TABLE_NAME);
    try (Table table = connection.getTable(tableName);
        ResultScanner rs = table.getScanner(getFragmentFamily())) {
      Iterator<Result> iterator = rs.iterator();
      int counter = 0;
      while (iterator.hasNext()) {
        Result r = iterator.next();
        ByteBuffer attemptValue = ByteBuffer.wrap(r.getValue(getFragmentFamily(), getAttemptQualifier()));
        byte[] datasetValue = r.getValue(getFragmentFamily(), getDatasetIdQualifier());
        byte[] recordValue = r.getValue(getFragmentFamily(), getRecordQualifier());

        Integer attemptInt = attemptValue.getInt();
        String datasetString = new String(datasetValue);
        String recordString = new String(recordValue);

        Assert.assertEquals(expectedDatasetId, datasetString);
        Assert.assertEquals(expectedAttempt, attemptInt);
        Assert.assertNotNull(recordString);
        Assert.assertTrue(recordString.length() > 0);

        counter++;
      }
      Assert.assertEquals(expectedSize, counter);
    }
  }

}
