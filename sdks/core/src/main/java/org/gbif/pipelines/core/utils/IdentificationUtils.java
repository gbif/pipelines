package org.gbif.pipelines.core.utils;

import static org.gbif.dwc.terms.DwcTerm.GROUP_IDENTIFICATION;
import static org.gbif.dwc.terms.DwcTerm.GROUP_TAXON;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValue;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Utility class to help with the extraction of the identification fields. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IdentificationUtils {

  private static final Set<Term> IDENTIFICATION_FIELDS =
      Arrays.stream(DwcTerm.values())
          .filter(
              t ->
                  (t.getGroup().equals(GROUP_IDENTIFICATION) || t.getGroup().equals(GROUP_TAXON))
                      && !t.isClass())
          .collect(Collectors.toSet());

  private static final TemporalParser TEMPORAL_PARSER = DateParsers.defaultTemporalParser();

  public static Map<String, String> getIdentificationFieldTermsSource(ExtendedRecord er) {
    return areAllIdentificationFieldsNull(er)
        ? getLatestIdentificationExtension(er).orElse(new HashMap<>())
        : er.getCoreTerms();
  }

  public static String extractFromIdentificationExtension(ExtendedRecord er, Term term) {
    if (IDENTIFICATION_FIELDS.contains(term)
        && ModelUtils.hasExtension(er, Extension.IDENTIFICATION)
        && areAllIdentificationFieldsNull(er)) {
      return getLatestIdentificationExtension(er)
          .map(ext -> ext.get(term.qualifiedName()))
          .orElse(null);
    }
    return null;
  }

  private static Optional<Map<String, String>> getLatestIdentificationExtension(ExtendedRecord er) {
    if (!ModelUtils.hasExtension(er, Extension.IDENTIFICATION)) {
      return Optional.empty();
    }

    List<Map<String, String>> identificationExtension =
        er.getExtensions().get(DwcTerm.Identification.qualifiedName());
    if (identificationExtension.size() > 1
        && identificationExtension.stream()
            .allMatch(v -> hasValue(v.get(DwcTerm.dateIdentified.qualifiedName())))) {
      // if there are multiple extensions we choose the most recent one but only if we can
      // determine it. Therefore, if there are extensions without date we discard them all
      Map<String, String> latestIdentification = null;
      Long latestTime = null;
      for (Map<String, String> ext : identificationExtension) {
        String rawDateIdentified = ext.get(DwcTerm.dateIdentified.qualifiedName());
        TemporalAccessor result = TEMPORAL_PARSER.parse(rawDateIdentified).getPayload();

        Optional<Long> time = getTime(result);
        if (!time.isPresent()) {
          // since we can't determine the time of this extension we can't determine which extension
          // is the most recent
          return Optional.empty();
        }

        if (latestIdentification == null || time.get() > latestTime) {
          latestIdentification = ext;
          latestTime = time.get();
        }
      }

      return Optional.ofNullable(latestIdentification);
    } else if (identificationExtension.size() == 1) {
      return Optional.of(identificationExtension.get(0));
    }

    return Optional.empty();
  }

  /**
   * This method is needed because we follow an all-or-nothing approach, so we take values from the
   * identification extension only if all the identification fields in the core are null.
   */
  private static boolean areAllIdentificationFieldsNull(ExtendedRecord er) {
    return IDENTIFICATION_FIELDS.stream()
        .noneMatch(f -> hasValue(er.getCoreTerms().get(f.qualifiedName())));
  }

  private static Optional<Long> getTime(TemporalAccessor temporalAccessor) {
    if (temporalAccessor == null) {
      return Optional.empty();
    }

    int year;
    if (temporalAccessor.isSupported(ChronoField.YEAR)) {
      year = temporalAccessor.get(ChronoField.YEAR);
    } else {
      return Optional.empty();
    }

    int month = 1;
    if (temporalAccessor.isSupported(ChronoField.MONTH_OF_YEAR)) {
      month = temporalAccessor.get(ChronoField.MONTH_OF_YEAR);
    }

    int day = 1;
    if (temporalAccessor.isSupported(ChronoField.DAY_OF_MONTH)) {
      day = temporalAccessor.get(ChronoField.DAY_OF_MONTH);
    }

    long time = LocalDate.of(year, month, day).toEpochDay();
    if (temporalAccessor.isSupported(ChronoField.NANO_OF_DAY)) {
      time += temporalAccessor.getLong(ChronoField.NANO_OF_DAY);
    }

    return Optional.of(time);
  }
}
