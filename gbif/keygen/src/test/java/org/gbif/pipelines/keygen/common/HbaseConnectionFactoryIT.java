package org.gbif.pipelines.keygen.common;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.pipelines.keygen.HbaseServer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

public class HbaseConnectionFactoryIT {

  @ClassRule public static final HbaseServer HBASE_SERVER = HbaseServer.getInstance();

  private final Supplier<CompletableFuture<Connection>> connectionAsyncSupplier =
      () ->
          CompletableFuture.supplyAsync(
              HbaseConnectionFactory.getInstance(HBASE_SERVER.getZKString())::getConnection);

  @Test
  public void instanceTest() throws IOException {

    // When
    Connection conn1 =
        HbaseConnectionFactory.getInstance(HBASE_SERVER.getZKString()).getConnection();
    Connection conn2 =
        HbaseConnectionFactory.getInstance(HBASE_SERVER.getZKString()).getConnection();

    // Should
    Assert.assertSame(conn1, conn2);

    // Post action
    conn1.close();
  }

  @Test
  public void closeInstanceTest() throws IOException {
    // When
    Connection conn1 =
        HbaseConnectionFactory.getInstance(HBASE_SERVER.getZKString()).getConnection();
    Connection conn2 =
        HbaseConnectionFactory.getInstance(HBASE_SERVER.getZKString()).getConnection();

    conn1.close();

    Connection conn3 =
        HbaseConnectionFactory.getInstance(HBASE_SERVER.getZKString()).getConnection();

    // Should
    Assert.assertTrue(conn1.isClosed());
    Assert.assertTrue(conn2.isClosed());
    Assert.assertSame(conn1, conn2);
    Assert.assertNotSame(conn1, conn3);
    Assert.assertFalse(conn3.isClosed());

    // Post action
    conn3.close();
  }

  @Test
  public void asyncInstanceTest() throws Exception {
    // When
    CompletableFuture<Connection> cf1 = connectionAsyncSupplier.get();
    CompletableFuture<Connection> cf2 = connectionAsyncSupplier.get();
    CompletableFuture<Connection> cf3 = connectionAsyncSupplier.get();

    Connection conn1 = cf1.get();
    Connection conn2 = cf2.get();
    Connection conn3 = cf3.get();

    // Should
    Assert.assertSame(conn1, conn2);
    Assert.assertSame(conn1, conn3);

    // Post action
    conn1.close();
  }

  @Test
  public void asyncCloseInstanceTest() throws Exception {
    // When
    CompletableFuture<Connection> cf1 = connectionAsyncSupplier.get();
    CompletableFuture<Connection> cf2 = connectionAsyncSupplier.get();

    Connection conn1 = cf1.get();
    Connection conn2 = cf2.get();

    conn1.close();

    CompletableFuture<Connection> cf3 = connectionAsyncSupplier.get();

    Connection conn3 = cf3.get();

    // Should
    Assert.assertTrue(conn1.isClosed());
    Assert.assertTrue(conn2.isClosed());
    Assert.assertSame(conn1, conn2);
    Assert.assertNotSame(conn1, conn3);
    Assert.assertFalse(conn3.isClosed());

    // Post action
    conn3.close();
  }
}
