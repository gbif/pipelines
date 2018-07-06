package org.gbif.pipelines.indexing.converter;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.taxon.Rank;
import org.gbif.pipelines.io.avro.taxon.RankedName;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.avro.specific.SpecificRecordBase;

/**
 * Converter for objects to GBIF elasticsearch schema. You must pass: {@link ExtendedRecord}, {@link
 * org.gbif.pipelines.io.avro.InterpretedExtendedRecord}, {@link
 * org.gbif.pipelines.io.avro.temporal.TemporalRecord}, {@link
 * LocationRecord}, {@link TaxonRecord}, {@link
 * org.gbif.pipelines.io.avro.multimedia.MultimediaRecord}
 *
 * <pre>{@code
 * Usage example:
 *
 * InterpretedExtendedRecord interRecord = ...
 * TemporalRecord temporal =  ...
 * LocationRecord location =  ...
 * TaxonRecord taxon =  ...
 * MultimediaRecord multimedia =  ...
 * ExtendedRecord extendedRecord =  ...
 * String result = GbifRecords2JsonConverter.create(extendedRecord, interRecord, temporal, location, taxon, multimedia).buildJson();
 *
 * }</pre>
 */
public class GbifRecords2JsonConverter extends Records2JsonConverter {

  private static final String[] ESCAPE_KEYS = {
    "decimalLatitude", "decimalLongitude", "diagnostics", "id"
  };
  private static final String[] REPLACE_KEYS = {
    "http://rs.tdwg.org/dwc/terms/", "http://purl.org/dc/terms/"
  };

  private GbifRecords2JsonConverter(SpecificRecordBase[] bases) {
    setSpecificRecordBase(bases);
    setEscapeKeys(ESCAPE_KEYS);
    setReplaceKeys(REPLACE_KEYS);
    addSpecificConverter(ExtendedRecord.class, getExtendedRecordConverter());
    addSpecificConverter(LocationRecord.class, getLocationRecordConverter());
    addSpecificConverter(TaxonRecord.class, getTaxonomyRecordConverter());
  }

  public static GbifRecords2JsonConverter create(SpecificRecordBase... bases) {
    return new GbifRecords2JsonConverter(bases);
  }

  /**
   * String converter for {@link ExtendedRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "verbatim": {
   *   "continent": "North America",
   *   //.....more fields
   * },
   * "basisOfRecord": null,
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<SpecificRecordBase, StringBuilder> getExtendedRecordConverter() {
    return (record, sb) -> {
      Map<String, String> terms = ((ExtendedRecord) record).getCoreTerms();
      addJsonFieldNoCheck("id", record.get(0));
      // Create verbatim object
      append("\"verbatim\":{");
      terms.forEach(this::addJsonField);
      append("},");
    };
  }

  /**
   * String converter for {@link LocationRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "location": {"lon": 10, "lat": 10},
   * "continent": "NORTH_AMERICA",
   * "waterBody": null,
   *  //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<SpecificRecordBase, StringBuilder> getLocationRecordConverter() {
    return (record, sb) -> {
      LocationRecord location = (LocationRecord) record;
      append("\"location\":");
      if (Objects.isNull(location.getDecimalLongitude())
          || Objects.isNull(location.getDecimalLatitude())) {
        append("null,");
      } else {
        append("{");
        addJsonField("lon", location.getDecimalLongitude());
        addJsonField("lat", location.getDecimalLatitude());
        append("},");
      }
      // Fields as a common view - "key": "value"
      commonConvert(record);
    };
  }

  /**
   * String converter for {@link TaxonRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "gbifKingdom": "Animalia",
   *  //.....more fields
   * "usage": {
   *  "key": 2442896,
   *  "name": "Actinemys marmorata (Baird & Girard, 1852)",
   *  "rank": "SPECIES"
   * },
   * "classification": [
   *  {
   *    "key": 1,
   *    "name": "Animalia",
   *    "rank": "KINGDOM"
   *  },
   *  //.....more objects
   * ],
   * "acceptedUsage": null,
   * //.....more fields
   *
   * }</pre>
   */
  private BiConsumer<SpecificRecordBase, StringBuilder> getTaxonomyRecordConverter() {
    return (record, sb) -> {
      TaxonRecord taxon = (TaxonRecord) record;
      Map<Rank, String> map =
          taxon
              .getClassification()
              .stream()
              .collect(Collectors.toMap(RankedName::getRank, RankedName::getName));
      // Gbif fields from map
      addJsonField("gbifKingdom", map.get(Rank.KINGDOM));
      addJsonField("gbifPhylum", map.get(Rank.PHYLUM));
      addJsonField("gbifClass", map.get(Rank.CLASS));
      addJsonField("gbifOrder", map.get(Rank.ORDER));
      addJsonField("gbifFamily", map.get(Rank.FAMILY));
      addJsonField("gbifGenus", map.get(Rank.GENUS));
      addJsonField("gbifSubgenus", map.get(Rank.SUBGENUS));
      // Other Gbif fields
      addJsonField("gbifSpeciesKey", taxon.getUsage().getKey());
      addJsonField("gbifScientificName", taxon.getUsage().getName());
      // Fields as a common view - "key": "value"
      commonConvert(record);
    };
  }
}
