package org.gbif.pipelines.hbase.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.client.Result;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Utility class to convert from HBase results and {@link VerbatimOccurrence} records into {@link
 * ExtendedRecord}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceConverter {

  /**
   * Converts a Hbase result into a {@link VerbatimOccurrence} record.
   *
   * @param row Hbase result
   * @return a {@link VerbatimOccurrence}
   */
  public static VerbatimOccurrence toVerbatimOccurrence(Result row) {
    return OccurrenceBuilder.buildVerbatimOccurrence(row);
  }

  /**
   * Converts a Hbase result into a {@link ExtendedRecord} record.
   *
   * @param row Hbase result
   * @return a {@link ExtendedRecord}
   */
  public static ExtendedRecord toExtendedRecord(Result row) {
    return toExtendedRecord(OccurrenceBuilder.buildVerbatimOccurrence(row));
  }

  /**
   * Converts a {@link VerbatimOccurrence} into a {@link ExtendedRecord} record.
   *
   * @return a {@link ExtendedRecord}
   */
  public static ExtendedRecord toExtendedRecord(VerbatimOccurrence verbatimOccurrence) {
    ExtendedRecord.Builder builder =
        ExtendedRecord.newBuilder()
            .setId(String.valueOf(verbatimOccurrence.getKey()))
            .setCoreTerms(toVerbatimMap(verbatimOccurrence.getVerbatimFields()));

    Optional.ofNullable(verbatimOccurrence.getExtensions())
        .ifPresent(ex -> builder.setExtensions(toVerbatimExtensionsMap(ex)));

    return builder.build();
  }

  /** Transforms a Map<Term,String> into Map<Term.qualifiedName/String,String>. */
  private static Map<String, String> toVerbatimMap(Map<Term, String> verbatimMap) {
    Map<String, String> rawMap = new HashMap<>();
    verbatimMap.forEach((k, v) -> rawMap.put(k.qualifiedName(), v));
    return rawMap;
  }

  /**
   * Transforms a Map<Extension, List<Map<Term, String>>> verbatimExtensions into
   * Map<Extension.getRowType()/String, List<Map<Term.qualifiedName/String, String>>>
   * verbatimExtensions.
   */
  private static Map<String, List<Map<String, String>>> toVerbatimExtensionsMap(
      Map<Extension, List<Map<Term, String>>> verbatimExtensions) {
    Map<String, List<Map<String, String>>> rawExtensions = new HashMap<>();
    verbatimExtensions.forEach(
        (k, v) ->
            rawExtensions.put(
                k.getRowType(),
                v.stream().map(OccurrenceConverter::toVerbatimMap).collect(Collectors.toList())));
    return rawExtensions;
  }
}
