package org.gbif.pipelines.core.converters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static Map<String, String> convertToMap(Record record) {
    Map<String, String> map = new HashMap<>(record.terms().size() / 2);
    for (Term term : record.terms()) {
      String qn = term.qualifiedName();
      if (qn != null) {
        String value = record.value(term);
        if (value != null) {
          map.put(qn, value);
        }
      }
    }
    return map;
  }

  /** Converts {@link StarRecord} to {@link ExtendedRecord} */
  public static ExtendedRecord from(Record core, Map<Term, List<Record>> extensions) {
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder();
    Optional.ofNullable(core.rowType()).ifPresent(x -> builder.setCoreRowType(x.qualifiedName()));
    builder.setCoreTerms(convertToMap(core));
    builder.setExtensions(
        extensions.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().qualifiedName(),
                    entry ->
                        entry.getValue().stream()
                            .map(ExtendedRecordConverter::convertToMap)
                            .collect(Collectors.toList()))));

    builder.setId(getId(core, builder));
    return builder.build();
  }

  /** If id is null, use triplet as an id */
  private static String getId(Record core, ExtendedRecord.Builder builder) {
    if (core.id() != null) {
      return core.id();
    }

    ExtendedRecord partial = builder.build(); // build the partial record to access core terms
    String institutionCode = partial.getCoreTerms().get(DwcTerm.institutionCode.qualifiedName());
    String collectionCode = partial.getCoreTerms().get(DwcTerm.collectionCode.qualifiedName());
    String catalogNumber = partial.getCoreTerms().get(DwcTerm.catalogNumber.qualifiedName());

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
