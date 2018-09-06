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
import java.util.stream.Collectors;

/** Converter from some class to {@link ExtendedRecord} */
public class ExtendedRecordConverter {

  private ExtendedRecordConverter() {}

  public static ExtendedRecord from(StarRecord record) {
    Record core = record.core();
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(core.id());
    Optional.ofNullable(core.rowType()).ifPresent(x -> builder.setCoreRowType(x.qualifiedName()));

    builder.setCoreTerms(removeEmptyContent(core));

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
                  extensionRecord -> extensionData.add(removeEmptyContent(extensionRecord)));

              builder.getExtensions().put(extensionType.qualifiedName(), extensionData);
            });

    return builder.build();
  }

  private static Map<String, String> removeEmptyContent(Record record) {
    return record
        .terms()
        .stream()
        .filter(term -> term.qualifiedName() != null && record.value(term) != null)
        .collect(Collectors.toMap(Term::qualifiedName, record::value, (a, b) -> b));
  }
}
