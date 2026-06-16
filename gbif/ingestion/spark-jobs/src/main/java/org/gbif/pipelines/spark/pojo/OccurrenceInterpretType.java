package org.gbif.pipelines.spark.pojo;

import java.util.Locale;
import java.util.Optional;

/** Enumeration of special interpretation runs that can be requested via the CLI/args. */
public enum OccurrenceInterpretType {
  MULTI_TAXONOMY,
  CLUSTERING,
  REGEN_OUTPUTS,
  REGEN_JSON_OUTPUTS,
  REGEN_HDFS_OUTPUTS;

  /**
   * Parse a string into an {@link OccurrenceInterpretType} in a case-insensitive manner.
   *
   * @param value the string value to parse
   * @return Optional containing the InterpretType if matched, otherwise empty
   */
  public static Optional<OccurrenceInterpretType> fromString(String value) {
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(OccurrenceInterpretType.valueOf(value.trim().toUpperCase(Locale.ROOT)));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
