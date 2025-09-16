package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.api.vocabulary.OccurrenceIssue.COUNTRY_INVALID;
import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.createNameUsageMatchRequest;
import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.createTaxonRecord;
import static org.gbif.pipelines.core.parsers.location.parser.LocationParser.parseCountry;
import static org.gbif.pipelines.core.utils.ModelUtils.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.parsers.VocabularyParser;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.parsers.location.parser.LocationParser;
import org.gbif.pipelines.core.parsers.location.parser.ParsedLocation;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse;

/**
 * Interpreter for taxonomic fields present in an {@link ExtendedRecord} avro file. These fields
 * should be based on the Darwin Core specification (http://rs.tdwg.org/dwc/terms/).
 *
 * <p>The interpretation uses the species match kv store to match the taxonomic fields to an
 * existing species.
 *
 * <p>The interpretation will match against each of the configured taxonomies.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MultiTaxonomyInterpreter {

  /**
   * Interprets a utils from the taxonomic fields specified in the {@link ExtendedRecord} received.
   */
  public static BiConsumer<ExtendedRecord, MultiTaxonRecord> interpretMultiTaxonomy(
      KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore,
      KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore,
      List<String> checklistKeys,
      Map<String, String> countryChecklistKeyMap) {
    return (er, mtr) -> {
      if (kvStore == null) {
        return;
      }

      ModelUtils.checkNullOrEmpty(er);
      final List<TaxonRecord> trs = new ArrayList<>();

      for (String checklistKey : checklistKeys) {
        final NameUsageMatchRequest nameUsageMatchRequest =
            createNameUsageMatchRequest(er, checklistKey);
        TaxonRecord taxonRecord =
            TaxonRecord.newBuilder().setId(er.getId()).setDatasetKey(checklistKey).build();
        createTaxonRecord(nameUsageMatchRequest, kvStore, taxonRecord);
        trs.add(taxonRecord);
      }

      // country specific checklist keys
      if (countryChecklistKeyMap != null
          && !countryChecklistKeyMap.isEmpty()
          && geocodeKvStore != null) {

        AtomicReference<String> isoCountryCode = new AtomicReference<>();

        // if country is in the er, no need to do geocode lookup
        ParsedField<Country> parsedCountry =
            parseCountry(er, VocabularyParser.countryParser(), COUNTRY_INVALID.name());

        if (parsedCountry.isSuccessful() && parsedCountry.getResult() != null) {
          isoCountryCode.set(parsedCountry.getResult().getIso2LetterCode());
        }

        // if countrycode is in the er, no need to do geocode lookup
        if (isoCountryCode.get() == null) {
          // Parse country code
          ParsedField<Country> parsedCountryCode =
              parseCountry(er, VocabularyParser.countryCodeParser(), COUNTRY_INVALID.name());
          if (parsedCountryCode.isSuccessful() && parsedCountryCode.getResult() != null) {
            isoCountryCode.set(parsedCountryCode.getResult().getIso2LetterCode());
          }
        }

        // if we dont have country code, do a geocode lookup
        if (isoCountryCode.get() == null) {

          // do the geocode lookup as we don't have ISO country code yet
          ParsedField<ParsedLocation> parsedResult = LocationParser.parse(er, geocodeKvStore);

          // set values in the location record
          ParsedLocation parsedLocation = parsedResult.getResult();

          Optional.ofNullable(parsedLocation.getCountry())
              .ifPresent(
                  country -> {
                    isoCountryCode.set(country.getIso2LetterCode());
                  });
        }

        // if we have a isoCountryCode and there is a mapping for it, do the name usage match
        if (isoCountryCode.get() != null
            && countryChecklistKeyMap.containsKey(isoCountryCode.get())) {
          String checklistKey = countryChecklistKeyMap.get(isoCountryCode.get());
          final NameUsageMatchRequest nameUsageMatchRequest =
              createNameUsageMatchRequest(er, checklistKey);
          TaxonRecord taxonRecord =
              TaxonRecord.newBuilder().setId(er.getId()).setDatasetKey(checklistKey).build();
          createTaxonRecord(nameUsageMatchRequest, kvStore, taxonRecord);
          trs.add(taxonRecord);
        }
      }

      mtr.setId(er.getId());
      mtr.setTaxonRecords(trs);
      setCoreId(er, mtr);
      setParentEventId(er, mtr);
    };
  }

  /** Sets the coreId field. */
  public static void setCoreId(ExtendedRecord er, MultiTaxonRecord mtr) {
    Optional.ofNullable(er.getCoreId()).ifPresent(mtr::setCoreId);
  }

  /** Sets the parentEventId field. */
  public static void setParentEventId(ExtendedRecord er, MultiTaxonRecord mtr) {
    extractOptValue(er, DwcTerm.parentEventID).ifPresent(mtr::setParentId);
  }
}
