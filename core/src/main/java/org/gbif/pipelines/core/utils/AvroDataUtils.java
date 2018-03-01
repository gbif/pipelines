package org.gbif.pipelines.core.utils;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Objects;

public final class AvroDataUtils {

  private AvroDataUtils() {
    // Do nothing
  }

  /**
   * Checks if a {@link ExtendedRecord} is null or empty.
   */
  public static void checkNullOrEmpty(ExtendedRecord extendedRecord) {
    Objects.requireNonNull(extendedRecord, "ExtendedRecord cannot be null");

    if (extendedRecord.getId() == null || extendedRecord.getId().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord with id is required");
    }

    if (extendedRecord.getCoreTerms() == null || extendedRecord.getCoreTerms().isEmpty()) {
      throw new IllegalArgumentException("ExtendedRecord with core terms is required");
    }
  }

}
