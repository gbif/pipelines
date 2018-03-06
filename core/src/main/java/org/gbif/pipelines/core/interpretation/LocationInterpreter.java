package org.gbif.pipelines.core.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.IssueType;
import org.gbif.pipelines.core.interpretation.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Location;

import java.util.function.Function;

public interface LocationInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * {@link DwcTerm#country} interpretation.
   */
  static LocationInterpreter interpretCountry(Location locationRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .countryParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            locationRecord.setCountry(parseResult.getPayload().name());
          } else {
            interpretation.withValidation(Interpretation.Trace.of(DwcTerm.country.name(), IssueType.COUNTRY_INVALID));
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#countryCode} interpretation.
   */
  static LocationInterpreter interpretCountryCode(Location locationRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .countryParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            locationRecord.setCountryCode(parseResult.getPayload().name());
          } else {
            interpretation.withValidation(Interpretation.Trace.of(DwcTerm.countryCode.name(), IssueType.COUNTRY_INVALID));
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#continent} interpretation.
   */
  static LocationInterpreter interpretContinent(Location locationRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .continentParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            locationRecord.setContinent(parseResult.getPayload().name());
          } else {
            interpretation.withValidation(Interpretation.Trace.of(DwcTerm.continent.name(), IssueType.CONTINENT_INVALID));
          }
          return interpretation;
        }).get();
  }
}
