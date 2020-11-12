package org.gbif.pipelines.core.converters;

import com.google.common.base.Strings;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * In case of sampling event occurrence records stored in extensions, this converter extracts
 * occurrence records {@link DwcTerm#Occurrence} from extension and returns them as list of {@link
 * ExtendedRecord}
 *
 * @see <a href="https://github.com/gbif/ipt/wiki/BestPracticesSamplingEventData>Sampling event</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceExtensionConverter {

  public static Optional<ExtendedRecord> convert(
      Map<String, String> coreMap, Map<String, String> extMap) {
    String id = extMap.get(DwcTerm.occurrenceID.qualifiedName());
    if (!Strings.isNullOrEmpty(id)) {
      ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(id).build();
      extendedRecord.getCoreTerms().putAll(coreMap);
      extendedRecord.getCoreTerms().putAll(extMap);
      return Optional.of(extendedRecord);
    }
    return Optional.empty();
  }
}
