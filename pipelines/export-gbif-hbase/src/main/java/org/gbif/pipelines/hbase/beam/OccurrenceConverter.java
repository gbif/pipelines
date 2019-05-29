package org.gbif.pipelines.hbase.beam;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.hadoop.hbase.client.Result;

/**
 * Utility class to convert from HBase results and {@link VerbatimOccurrence} records into {@link ExtendedRecord}.
 */
class OccurrenceConverter {

  /**
   * Private constructor.
   */
  private OccurrenceConverter() {
    //do nothing
  }

  /**
   * Converts a Hbase result into a {@link VerbatimOccurrence} record.
   * @param row Hbase result
   * @return a {@link VerbatimOccurrence}
   */
  static VerbatimOccurrence toVerbatimOccurrence(Result row) {
    return OccurrenceBuilder.buildVerbatimOccurrence(row);
  }

  /**
   * Converts a Hbase result into a {@link ExtendedRecord} record.
   * @param row Hbase result
   * @return a {@link ExtendedRecord}
   */
  static ExtendedRecord toExtendedRecord(Result row) {
    return toExtendedRecord(OccurrenceBuilder.buildVerbatimOccurrence(row));
  }

  /**
   * Converts a {@link VerbatimOccurrence} into a {@link ExtendedRecord} record.
   * @param row Hbase result
   * @return a {@link ExtendedRecord}
   */
  static ExtendedRecord toExtendedRecord(VerbatimOccurrence verbatimOccurrence) {
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder()
      .setId(String.valueOf(verbatimOccurrence.getKey()))
      .setCoreTerms(toVerbatimMap(verbatimOccurrence.getVerbatimFields()));
    if (Objects.nonNull(verbatimOccurrence.getExtensions())) {
      builder.setExtensions(toVerbatimExtensionsMap(verbatimOccurrence.getExtensions()));
    }
    return builder.build();
  }

  /**
   * Transforms a Map<Term,String> into Map<Term.qualifiedName/String,String>.
   */
  private static Map<String, String> toVerbatimMap(Map<Term,String> verbatimMap) {
    Map<String, String> rawMap = new HashMap<>();
    verbatimMap.forEach((k,v) -> rawMap.put(k.qualifiedName(), v));
    return rawMap;
  }

  /**
   * Transforms a Map<Extension, List<Map<Term, String>>> verbatimExtensions into Map<Extension.getRowType()/String, List<Map<Term.qualifiedName/String, String>>> verbatimExtensions.
   */
  private static Map<String, List<Map<String, String>>> toVerbatimExtensionsMap(Map<Extension, List<Map<Term, String>>> verbatimExtensions) {
    Map<String, List<Map<String, String>>> rawExtensions = new HashMap<>();
    verbatimExtensions
            .forEach((k,v) -> rawExtensions.put(k.getRowType(),
                                                v.stream().map(OccurrenceConverter::toVerbatimMap)
                                                          .collect(Collectors.toList())));
    return rawExtensions;
  }
}
