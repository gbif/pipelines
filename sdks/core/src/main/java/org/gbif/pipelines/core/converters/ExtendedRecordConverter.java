package org.gbif.pipelines.core.converters;

import org.gbif.dwc.record.Record;
import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Converters from *.class to {@link ExtendedRecord} */
public class ExtendedRecordConverter {

  private ExtendedRecordConverter() {}

  /** Converts {@link StarRecord} to {@link ExtendedRecord} */
  public static ExtendedRecord from(StarRecord record) {
    Record core = record.core();
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(core.id());
    Optional.ofNullable(core.rowType()).ifPresent(x -> builder.setCoreRowType(x.qualifiedName()));

    Function<Record, Map<String, String>> removeEmptyContent =
        r ->
            r.terms()
                .stream()
                .filter(term -> term.qualifiedName() != null && r.value(term) != null)
                .collect(Collectors.toMap(Term::qualifiedName, r::value, (a, b) -> b));

    builder.setCoreTerms(removeEmptyContent.apply(core));

    record
        .extensions()
        .forEach(
            (extensionType, data) -> {
              if (builder.getExtensions() == null) {
                builder.setExtensions(new HashMap<>());
              }

              List<Map<String, String>> extensionData =
                  builder
                      .getExtensions()
                      .getOrDefault(extensionType.qualifiedName(), new ArrayList<>());

              data.forEach(
                  extensionRecord -> extensionData.add(removeEmptyContent.apply(extensionRecord)));

              builder.getExtensions().put(extensionType.qualifiedName(), extensionData);
            });

    return builder.build();
  }
}
