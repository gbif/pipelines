package org.gbif.data.pipelines.io.dwca;

import org.gbif.data.io.avro.ExtendedRecord;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.record.StarRecord;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedRecords {
  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecord.class);

  /**
   * Builds a new instance of an ExtendedRecord for the given StarRecord.
   */
  public static ExtendedRecord newFromStarRecord(StarRecord record) {
    ExtendedRecord.Builder builder = ExtendedRecord.newBuilder()
      .setId(record.core().id());
    builder.setCoreTerms(new HashMap());

    for (Term term : record.core().terms()) {
      builder.getCoreTerms().put(term.qualifiedName(), record.core().value(term));
    }

    record.extensions().forEach((extensionType, data) -> {

      List<Map<CharSequence,CharSequence>> extensionData =
        builder.getExtensions().getOrDefault(extensionType.qualifiedName(), Collections.emptyList());

      data.forEach(extensionRecord -> {
        Map<CharSequence, CharSequence> extensionRecordTerms = new HashMap<CharSequence, CharSequence>();
        for (Term term : extensionRecord.terms()) {
          System.out.println("EXTENSION: " + term.qualifiedName() + ": " + record.core().value(term));
          extensionRecordTerms.put(term.qualifiedName(), extensionRecord.value(term));
        }
        extensionData.add(extensionRecordTerms);

      });

      builder.getExtensions().put(extensionType.qualifiedName(), extensionData);
    });

    return builder.build();
  }
}
