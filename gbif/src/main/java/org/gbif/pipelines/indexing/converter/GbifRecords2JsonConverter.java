package org.gbif.pipelines.indexing.converter;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.taxon.Rank;
import org.gbif.pipelines.io.avro.taxon.RankedName;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Converter for objects to GBIF elasticsearch schema. You must pass: {@link ExtendedRecord}, {@link
 * org.gbif.pipelines.io.avro.InterpretedExtendedRecord}, {@link
 * org.gbif.pipelines.io.avro.temporal.TemporalRecord}, {@link LocationRecord}, {@link TaxonRecord},
 * {@link org.gbif.pipelines.io.avro.multimedia.MultimediaRecord}
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

  private static final String[] SKIP_KEYS = {"id", "decimalLatitude", "decimalLongitude"};
  private static final String[] REPLACE_KEYS = {
    "http://rs.tdwg.org/dwc/terms/", "http://purl.org/dc/terms/"
  };

  private GbifRecords2JsonConverter(SpecificRecordBase[] bases) {
    this.setSpecificRecordBase(bases)
        .setSkipKeys(SKIP_KEYS)
        .setReplaceKeys(REPLACE_KEYS)
        .addSpecificConverter(ExtendedRecord.class, getExtendedRecordConverter())
        .addSpecificConverter(LocationRecord.class, getLocationRecordConverter())
        .addSpecificConverter(TaxonRecord.class, getTaxonomyRecordConverter());
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
  private Consumer<SpecificRecordBase> getExtendedRecordConverter() {
    return record -> {
      Map<String, String> terms = ((ExtendedRecord) record).getCoreTerms();
      this.addJsonFieldNoCheck("id", record.get(0).toString()).addJsonObject("verbatim", terms);
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
  private Consumer<SpecificRecordBase> getLocationRecordConverter() {
    return record -> {
      LocationRecord location = (LocationRecord) record;

      if (location.getDecimalLongitude() != null && location.getDecimalLatitude() != null) {
        ObjectNode node = mapper.createObjectNode();
        node.put("lon", location.getDecimalLongitude().toString());
        node.put("lat", location.getDecimalLatitude().toString());
        this.addJsonObject("location", node);
      }
      // Fields as a common view - "key": "value"
      this.addCommonFields(record);
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
  private Consumer<SpecificRecordBase> getTaxonomyRecordConverter() {
    return record -> {
      TaxonRecord taxon = (TaxonRecord) record;

      List<RankedName> classifications = taxon.getClassification();
      if (classifications != null && !classifications.isEmpty()) {

        Map<Rank, String> map =
            classifications
                .stream()
                .collect(
                    Collectors.toMap(
                        RankedName::getRank, rankedName -> rankedName.getKey().toString()));

        // Gbif fields from map
        Optional.ofNullable(map.get(Rank.KINGDOM))
            .ifPresent(val -> this.addJsonField("gbifKingdomKey", val));
        Optional.ofNullable(map.get(Rank.PHYLUM))
            .ifPresent(val -> this.addJsonField("gbifPhylumKey", val));
        Optional.ofNullable(map.get(Rank.CLASS))
            .ifPresent(val -> this.addJsonField("gbifClassKey", val));
        Optional.ofNullable(map.get(Rank.ORDER))
            .ifPresent(val -> this.addJsonField("gbifOrderKey", val));
        Optional.ofNullable(map.get(Rank.FAMILY))
            .ifPresent(val -> this.addJsonField("gbifFamilyKey", val));
        Optional.ofNullable(map.get(Rank.GENUS))
            .ifPresent(val -> this.addJsonField("gbifGenusKey", val));
        Optional.ofNullable(map.get(Rank.SUBGENUS))
            .ifPresent(val -> this.addJsonField("gbifSubgenusKey", val));
        Optional.ofNullable(map.get(Rank.SPECIES))
            .ifPresent(val -> this.addJsonField("gbifSpeciesKey", val));
      }

      // Other Gbif fields
      RankedName usage = taxon.getUsage();
      if (usage != null) {
        this.addJsonField("gbifTaxonKey", usage.getKey().toString())
            .addJsonField("gbifScientificName", usage.getName())
            .addJsonField("gbifTaxonRank", usage.getRank().name());
      }
      // Fields as a common view - "key": "value"
      this.addCommonFields(record);
    };
  }
}
