package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import au.org.ala.pipelines.parser.CollectorNameParser;
import au.org.ala.pipelines.parser.LicenseParser;
import au.org.ala.pipelines.vocabulary.PreparationsParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.License;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Extensions to {@link org.gbif.pipelines.core.interpreters.core.BasicInterpreter} to support
 * living atlases.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALABasicInterpreter {

  public static final String UNKNOWN_PREPRARATIONS = "UNKNOWN";

  /** {@link DwcTerm#occurrenceStatus} interpretation. */
  public static BiConsumer<ExtendedRecord, BasicRecord> interpretRecordedBy(
      KeyValueStore<String, List<String>> recordedByKvStore) {
    return (er, br) -> {
      if (recordedByKvStore == null) {
        return;
      }
      extractOptValue(er, DwcTerm.recordedBy)
          .filter(x -> !x.isEmpty())
          .map(recordedByKvStore::get)
          .ifPresent(br::setRecordedBy);
    };
  }

  public static BiConsumer<ExtendedRecord, BasicRecord> interpretPreparations(
      PreparationsParser parser) {
    return (er, br) -> {
      Optional<String> value = extractOptValue(er, DwcTerm.preparations);
      if (value.isPresent()) {
        String[] parts = value.get().replaceAll("\"", "").split("\\|");
        List<String> parsedTokens = new ArrayList<String>();
        for (String part : parts) {
          ParseResult<String> results = parser.parse(part.trim());
          if (results.isSuccessful()) {
            parsedTokens.add(results.getPayload());
          }
        }

        // if there is a mixture of "Unknown" and others, strip out "Unknown"
        // to avoid "skull|Unknown|Spirit" values
        if (parsedTokens.size() > 1) {
          parsedTokens =
              parsedTokens.stream()
                  .filter(token -> !UNKNOWN_PREPRARATIONS.equalsIgnoreCase(token))
                  .collect(Collectors.toList());
        }

        br.setPreparations(parsedTokens);
      }
    };
  }

  public static void interpretRecordedBy(ExtendedRecord er, BasicRecord br) {
    extractOptValue(er, DwcTerm.recordedBy)
        .filter(x -> !x.isEmpty())
        .map(CollectorNameParser::parseList)
        .map(Arrays::asList)
        .ifPresent(br::setRecordedBy);
  }

  public static void interpretLicense(ExtendedRecord er, BasicRecord br) {
    LicenseParser parser = LicenseParser.getInstance();
    Optional<String> value = extractOptValue(er, DcTerm.license);
    if (value.isPresent()) {
      br.setLicense(parser.matchLicense(value.get()));
    } else {
      br.setLicense(License.UNSPECIFIED.name());
    }
  }
}
