package org.gbif.pipelines.core.converters;

import static org.gbif.dwc.terms.DwcTerm.Occurrence;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
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

  public static List<ExtendedRecord> convert(ExtendedRecord er) {

    Map<String, String> coreTerms = er.getCoreTerms();
    Map<String, Map<String, List<Map<String, String>>>> occIdExtMap = collectExtensions(er);

    return er.getExtensions().get(DwcTerm.Occurrence.qualifiedName()).stream()
        .map(occExt -> merge(er.getId(), coreTerms, occExt, occIdExtMap))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  /** Merge all maps into ExtendedRecord object */
  private static Optional<ExtendedRecord> merge(
      String coreId,
      Map<String, String> coreMap,
      Map<String, String> extCoreMap,
      Map<String, Map<String, List<Map<String, String>>>> occIdExtMap) {
    String id = extCoreMap.get(DwcTerm.occurrenceID.qualifiedName());
    if (!Strings.isNullOrEmpty(id)) {
      ExtendedRecord extendedRecord = ExtendedRecord.newBuilder().setId(id).build();
      extendedRecord.getCoreTerms().putAll(coreMap);
      extendedRecord.getCoreTerms().putAll(extCoreMap);
      extendedRecord.setCoreId(coreId);
      Optional.ofNullable(occIdExtMap.get(id)).ifPresent(extendedRecord::setExtensions);
      return Optional.of(extendedRecord);
    }
    return Optional.empty();
  }

  /** Extract all extension and map to occurrenceID */
  private static Map<String, Map<String, List<Map<String, String>>>> collectExtensions(
      ExtendedRecord er) {

    Map<String, Map<String, List<Map<String, String>>>> result = new HashMap<>();

    // Go through all extensions lists
    for (Entry<String, List<Map<String, String>>> entry : er.getExtensions().entrySet()) {
      String extensionName = entry.getKey();

      // Skip occurrence extension
      if (extensionName.equals(Occurrence.qualifiedName())) {
        continue;
      }

      // Final extension
      for (Map<String, String> rawExtensionData : entry.getValue()) {

        // Extract occurrenceID and use it as map key
        String occurrenceId = rawExtensionData.get(DwcTerm.occurrenceID.qualifiedName());

        Map<String, List<Map<String, String>>> parsedExtensions = result.get(occurrenceId);

        // If the map is null we create new map for the extension
        if (parsedExtensions == null) {
          Map<String, List<Map<String, String>>> m = new HashMap<>(er.getExtensions().size() - 1);
          m.put(extensionName, createList(rawExtensionData));
          result.put(occurrenceId, m);

          // Or we take existing extension list and new extension
        } else {
          List<Map<String, String>> listOfParsedExtension = parsedExtensions.get(extensionName);
          if (listOfParsedExtension == null) {
            // Avoid
            parsedExtensions.put(extensionName, createList(rawExtensionData));
          } else {
            listOfParsedExtension.add(rawExtensionData);
          }
        }
      }
    }

    return result.isEmpty() ? Collections.emptyMap() : result;
  }

  private static List<Map<String, String>> createList(Map<String, String> extensionData) {
    List<Map<String, String>> list = new ArrayList<>();
    list.add(extensionData);
    return list;
  }
}
