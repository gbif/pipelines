package org.gbif.pipelines.core.functions;

import org.gbif.dwc.terms.Term;
import org.gbif.dwca.record.StarRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ExtendedRecordBuilder implements SerializableFunction<StarRecord, ExtendedRecord> {

  @Override
  public ExtendedRecord apply(StarRecord record) {
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder()
                                                   .setId(record.core().id());
    builder.setCoreTerms(new HashMap<>());

    for (Term term : record.core().terms()) {
      // remove empty content
      if (term.qualifiedName() != null && record.core().value(term) != null) {
        builder.getCoreTerms().put(term.qualifiedName(), record.core().value(term));
      }
    }

    record.extensions().forEach((extensionType, data) -> {
      if (builder.getExtensions() == null) builder.setExtensions(new HashMap<>());

      List<Map<String, String>> extensionData =
        builder.getExtensions().getOrDefault(extensionType.qualifiedName(), new ArrayList<>());

      data.forEach(extensionRecord -> {
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
