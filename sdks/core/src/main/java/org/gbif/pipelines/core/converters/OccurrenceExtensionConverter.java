package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * In case of @see <a href="https://github.com/gbif/ipt/wiki/BestPracticesSamplingEventData>Sampling event</a>
 * occurrence records stored in extensions, this converter extracts occurrence records {@link DwcTerm#Occurrence} from
 * extension and returns them as list of {@link ExtendedRecord}
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceExtensionConverter {

  public static List<ExtendedRecord> convert(ExtendedRecord er) {
    List<Map<String, String>> occurrenceExts = er.getExtensions().get(DwcTerm.Occurrence.qualifiedName());
    if (occurrenceExts == null || occurrenceExts.isEmpty()) {
      return Collections.emptyList();
    }

    Map<String, String> coreTerms = er.getCoreTerms();

    return occurrenceExts.stream()
        .map(occurrence -> {
          String id = occurrence.get(DwcTerm.occurrenceID.qualifiedName());
          ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(id).build();
          extendedRecord.getCoreTerms().putAll(coreTerms);
          extendedRecord.getCoreTerms().putAll(occurrence);
          return extendedRecord;
        })
        .collect(Collectors.toList());
  }

}
