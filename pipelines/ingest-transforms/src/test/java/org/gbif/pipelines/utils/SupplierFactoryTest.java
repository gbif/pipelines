package org.gbif.pipelines.utils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

public class SupplierFactoryTest {

  @Test
  public void instanceTest() {

    Supplier<Long> supplier = () -> LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);

    Long long1 = SupplierFactory.getInstance(supplier).getService();
    Long long2 = SupplierFactory.getInstance(supplier).getService();
    Long long3 = SupplierFactory.getInstance(supplier).getService();

    Assert.assertEquals(long1, long2);
    Assert.assertEquals(long1, long3);
  }

  @Test
  public void asyncInstanceTest() throws Exception {

    Supplier<Long> supplier = () -> LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);

    Supplier<Long> longSupplier = () -> SupplierFactory.getInstance(supplier).getService();

    CompletableFuture<Long> r1 = CompletableFuture.supplyAsync(longSupplier);
    CompletableFuture<Long> r2 = CompletableFuture.supplyAsync(longSupplier);
    CompletableFuture<Long> r3 = CompletableFuture.supplyAsync(longSupplier);

    Long long1 = r1.get();
    Long long2 = r2.get();
    Long long3 = r3.get();

    Assert.assertEquals(long1, long2);
    Assert.assertEquals(long1, long3);
  }

}
