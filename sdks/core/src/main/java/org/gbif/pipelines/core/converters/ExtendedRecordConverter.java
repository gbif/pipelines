package org.gbif.pipelines.core.converters;

import org.gbif.dwc.record.StarRecord;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Converter from some class to {@link ExtendedRecord} */
public class ExtendedRecordConverter {

  private ExtendedRecordConverter() {}

  public static ExtendedRecord from(StarRecord record) {
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder().setId(record.core().id());
    Optional.ofNullable(record.core().rowType())
        .ifPresent(x -> builder.setCoreRowType(x.qualifiedName()));
    builder.setCoreTerms(new HashMap<>());

    // remove empty content
    record
        .core()
        .terms()
        .stream()
        .filter(term -> term.qualifiedName() != null && record.core().value(term) != null)
        .forEach(
            term -> builder.getCoreTerms().put(term.qualifiedName(), record.core().value(term)));

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
                  extensionRecord -> {
                    Map<String, String> extensionRecordTerms = new HashMap<>();
                    for (Term term : extensionRecord.terms()) {
                      // filter unusable content
                      if (term.qualifiedName() != null && extensionRecord.value(term) != null) {
                        extensionRecordTerms.put(term.qualifiedName(), extensionRecord.value(term));
                      }
                    }
                    extensionData.add(extensionRecordTerms);
                  });

              builder.getExtensions().put(extensionType.qualifiedName(), extensionData);
            });

    return builder.build();
  }
}
