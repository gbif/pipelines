package org.gbif.pipelines.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Location;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.interpretation.parsers.VocabularyParsers;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

public interface LocationInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * {@link DwcTerm#country} interpretation.
   */
  static LocationInterpreter interpretCountry(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.countryParser()
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
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.countryParser().map(extendedRecord, parseResult -> {
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
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.continentParser().map(extendedRecord, parseResult -> {
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      if (parseResult.isSuccessful()) {
        locationRecord.setContinent(parseResult.getPayload().name());
      } else {
        interpretation.withValidation(Interpretation.Trace.of(DwcTerm.continent.name(), IssueType.CONTINENT_INVALID));
      }
      return interpretation;
    }).get();
  }

  /**
   * {@link DwcTerm#waterBody} interpretation.
   */
  static LocationInterpreter interpretWaterBody(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.waterBodyParser()
      .map(extendedRecord, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if (parseResult.isSuccessful()) {
          locationRecord.setWaterBody(parseResult.getPayload().name());
        } else {
          interpretation.withValidation(Interpretation.Trace.of(DwcTerm.waterBody.name(), IssueType.WATER_BODY_INVALID));
        }
        return interpretation;
      })
      .get();
  }

  /**
   * {@link DwcTerm#minimumElevationInMeters} interpretation.
   */
  static LocationInterpreter interpretMinimumElevationInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.minimumElevationInMetersParser()
      .map(extendedRecord, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if (parseResult.isSuccessful()) {
          locationRecord.setMinimumElevationInMeters(parseResult.getPayload().name());
        } else {
          interpretation.withValidation(Interpretation.Trace.of(DwcTerm.minimumElevationInMeters.name(),
                                                                IssueType.MIN_ELEVATION_INVALID));
        }
        return interpretation;
      })
      .get();
  }

  /**
   * {@link DwcTerm#maximumElevationInMeters} interpretation.
   */
  static LocationInterpreter interpretMaximumElevationInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.maximumElevationInMetersParser()
      .map(extendedRecord, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if (parseResult.isSuccessful()) {
          locationRecord.setMaximumElevationInMeters(parseResult.getPayload().name());
        } else {
          interpretation.withValidation(Interpretation.Trace.of(DwcTerm.maximumElevationInMeters.name(),
                                                                IssueType.MAX_ELEVATION_INVALID));
        }
        return interpretation;
      })
      .get();
  }

  /**
   * {@link DwcTerm#minimumDepthInMeters} interpretation.
   */
  static LocationInterpreter interpretMinimumDepthInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.minimumDepthInMetersParser()
      .map(extendedRecord, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if (parseResult.isSuccessful()) {
          locationRecord.setMinimumDepthInMeters(parseResult.getPayload().name());
        } else {
          interpretation.withValidation(Interpretation.Trace.of(DwcTerm.minimumDepthInMeters.name(),
                                                                IssueType.MIN_DEPTH_INVALID));
        }
        return interpretation;
      })
      .get();
  }

  /**
   * {@link DwcTerm#maximumDepthInMeters} interpretation.
   */
  static LocationInterpreter interpretMaximumDepthInMeters(Location locationRecord) {
    return (ExtendedRecord extendedRecord) -> VocabularyParsers.maximumDepthInMetersParser()
      .map(extendedRecord, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if (parseResult.isSuccessful()) {
          locationRecord.setMaximumDepthInMeters(parseResult.getPayload().name());
        } else {
          interpretation.withValidation(Interpretation.Trace.of(DwcTerm.maximumDepthInMeters.name(),
                                                                IssueType.MAX_DEPTH_INVALID));
        }
        return interpretation;
      })
      .get();
  }
}
