package org.gbif.pipelines.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.interpretation.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

public interface LocationInterpreter extends Function<ExtendedRecord,Interpretation<ExtendedRecord>> {


  /**
   * {@link DwcTerm#basisOfRecord} interpretation.
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
            interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.COUNTRY_INVALID));
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#basisOfRecord} interpretation.
   */
  static LocationInterpreter interpretCountryCode(Location locationRecord) {
    return (ExtendedRecord extendedRecord) ->
      VocabularyParsers
        .countryParser()
        .map(extendedRecord, parseResult -> {
          Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
          if (parseResult.isSuccessful()) {
            locationRecord.setCountryCode(parseResult.getPayload().getIso3LetterCode());
          } else {
            interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.COUNTRY_INVALID));
          }
          return interpretation;
        }).get();
  }

  /**
   * {@link DwcTerm#basisOfRecord} interpretation.
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
            interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.CONTINENT_INVALID));
          }
          return interpretation;
        }).get();
  }
}
