package org.gbif.pipelines.core.converters;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Converters from *.class to {@link ExtendedRecord} */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtendedRecordConverter {

  private static final String RECORD_ID_ERROR = "RECORD_ID_ERROR";

  // Function that removes all the empty elements of a record
  private static final Function<Record, Map<String, String>> REMOVE_EMPTY_CONTENT =
      record ->
          record.terms().stream()
              .filter(term -> term.qualifiedName() != null && record.value(term) != null)
              .collect(Collectors.toMap(Term::qualifiedName, record::value, (a, b) -> b));

  /** Converts {@link StarRecord} to {@link ExtendedRecord} */
  public static ExtendedRecord from(StarRecord record) {
    Record core = record.core();
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder();
    Optional.ofNullable(core.rowType()).ifPresent(x -> builder.setCoreRowType(x.qualifiedName()));
    builder.setCoreTerms(REMOVE_EMPTY_CONTENT.apply(core));
    builder.setExtensions(
        record.extensions().entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().qualifiedName(),
                    entry ->
                        entry.getValue().stream()
                            .map(REMOVE_EMPTY_CONTENT)
                            .collect(Collectors.toList()))));

    builder.setId(getId(core, builder));
    return builder.build();
  }

  /** If id is null, use triplet as an id */
  private static String getId(Record core, ExtendedRecord.Builder builder) {
    if (core.id() != null) {
      return core.id();
    }

    String institutionCode = builder.getCoreTerms().get(DwcTerm.institutionCode.qualifiedName());
    String collectionCode = builder.getCoreTerms().get(DwcTerm.collectionCode.qualifiedName());
    String catalogNumber = builder.getCoreTerms().get(DwcTerm.catalogNumber.qualifiedName());

    if (institutionCode == null || collectionCode == null || catalogNumber == null) {
      return RECORD_ID_ERROR;
    }

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    return String.join(":", "urn:catalog", institutionCode, collectionCode, catalogNumber);
  }

  public static String getRecordIdError() {
    return RECORD_ID_ERROR;
  }
}
