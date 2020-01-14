package org.gbif.pipelines.utils;

import java.util.function.Supplier;

@SuppressWarnings("all")
public class SupplierFactory<T> {

  private final T service;
  private static volatile SupplierFactory instance;
  private static final Object MUTEX = new Object();

  private SupplierFactory(Supplier<T> supplier) {
    service = supplier.get();
  }

  public static <T> SupplierFactory<T> getInstance(Supplier<T> supplier) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new SupplierFactory(supplier);
        }
      }
    }
    return instance;
  }

  public T getService() {
    return service;
  }

}
