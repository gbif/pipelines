package org.gbif.pipelines.fragmenter.common;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getAttemptQualifier;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getDatasetKeyQualifier;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getDateCreatedQualifier;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getDateUpdatedQualifier;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getFragmentFamily;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getProtocolQualifier;
import static org.gbif.pipelines.fragmenter.common.HbaseStore.getRecordQualifier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.gbif.api.vocabulary.EndpointType;
import org.junit.Assert;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableAssert {

  public static void assertTable(
      Connection connection,
      int expectedSize,
      String expectedDatasetKey,
      Integer expectedAttempt,
      EndpointType expectedEndpointType)
      throws IOException {
    assertTable(
        connection,
        expectedSize,
        expectedDatasetKey,
        expectedAttempt,
        expectedAttempt,
        expectedEndpointType);
  }

  public static void assertTable(
      Connection connection,
      int expectedSize,
      String expectedDatasetKey,
      Integer expectedAttempt,
      Integer expectedUpdatedAttempt,
      EndpointType expectedEndpointType)
      throws IOException {
    TableName tableName = TableName.valueOf(HbaseServer.FRAGMENT_TABLE_NAME);
    try (Table table = connection.getTable(tableName);
        ResultScanner rs = table.getScanner(getFragmentFamily())) {
      Iterator<Result> iterator = rs.iterator();
      int counter = 0;
      while (iterator.hasNext()) {
        Result r = iterator.next();

        byte[] datasetValue = r.getValue(getFragmentFamily(), getDatasetKeyQualifier());
        ByteBuffer attemptValue =
            ByteBuffer.wrap(r.getValue(getFragmentFamily(), getAttemptQualifier()));
        byte[] protocolValue = r.getValue(getFragmentFamily(), getProtocolQualifier());
        byte[] recordValue = r.getValue(getFragmentFamily(), getRecordQualifier());
        ByteBuffer createdValue =
            ByteBuffer.wrap(r.getValue(getFragmentFamily(), getDateCreatedQualifier()));
        ByteBuffer updatedValue =
            ByteBuffer.wrap(r.getValue(getFragmentFamily(), getDateUpdatedQualifier()));

        String datasetString = new String(datasetValue, UTF_8);
        Integer attemptInt = attemptValue.getInt();
        String protocolString = new String(protocolValue, UTF_8);
        String recordString = new String(recordValue, UTF_8);
        long createdLong = createdValue.getLong();
        long updatedLong = updatedValue.getLong();

        Assert.assertEquals(expectedDatasetKey, datasetString);
        if (expectedAttempt.equals(attemptInt)) {
          Assert.assertEquals(expectedAttempt, attemptInt);
          Assert.assertEquals(updatedLong, createdLong);
        } else {
          Assert.assertEquals(expectedUpdatedAttempt, attemptInt);
          Assert.assertNotEquals(updatedLong, createdLong);
        }

        Assert.assertEquals(expectedEndpointType.name(), protocolString);
        Assert.assertNotNull(recordString);
        Assert.assertTrue(recordString.length() > 0);

        counter++;
      }
      Assert.assertEquals(expectedSize, counter);
    }
  }
}
