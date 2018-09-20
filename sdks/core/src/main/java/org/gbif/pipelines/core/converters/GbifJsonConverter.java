package org.gbif.pipelines.core.converters;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Converter for objects to GBIF elasticsearch schema. You must pass: {@link ExtendedRecord}, {@link
 * org.gbif.pipelines.io.avro.BasicRecord}, {@link org.gbif.pipelines.io.avro.TemporalRecord},
 * {@link LocationRecord}, {@link TaxonRecord}, {@link org.gbif.pipelines.io.avro.MultimediaRecord}
 *
 * <pre>{@code
 * Usage example:
 *
 * BasicRecord basic = ...
 * TemporalRecord temporal =  ...
 * LocationRecord location =  ...
 * TaxonRecord taxon =  ...
 * MultimediaRecord multimedia =  ...
 * ExtendedRecord extendedRecord =  ...
 * String result = GbifRecords2JsonConverter.create(extendedRecord, interRecord, temporal, location, taxon, multimedia).buildJson();
 *
 * }</pre>
 */
public class GbifJsonConverter extends JsonConverter {

  private static final String[] SKIP_KEYS = {"id", "decimalLatitude", "decimalLongitude"};
  private static final String[] REPLACE_KEYS = {"http://rs.tdwg.org/dwc/terms/", "http://purl.org/dc/terms/"};

  private GbifJsonConverter(SpecificRecordBase[] bases) {
    this.setSpecificRecordBase(bases)
        .setSkipKeys(SKIP_KEYS)
        .setReplaceKeys(REPLACE_KEYS)
        .addSpecificConverter(ExtendedRecord.class, getExtendedRecordConverter())
        .addSpecificConverter(LocationRecord.class, getLocationRecordConverter())
        .addSpecificConverter(TaxonRecord.class, getTaxonomyRecordConverter());
  }

  public static GbifJsonConverter create(SpecificRecordBase... bases) {
    return new GbifJsonConverter(bases);
  }

  /** Change the json result, merging all issues from records to one array */
  @Override
  public ObjectNode buildJson() {
    ObjectNode mainNode = super.buildJson();
    ArrayNode arrayNode = MAPPER.createArrayNode();
    Arrays.stream(getBases())
        .filter(Issues.class::isInstance)
        .flatMap(x -> ((Issues) x).getIssues().getIssueList().stream())
        .forEach(arrayNode::add);
    mainNode.set("issues", arrayNode);
    return mainNode;
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
        ObjectNode node = MAPPER.createObjectNode();
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
            .ifPresent(v -> this.addJsonField("gbifKingdomKey", v));
        Optional.ofNullable(map.get(Rank.PHYLUM))
            .ifPresent(v -> this.addJsonField("gbifPhylumKey", v));
        Optional.ofNullable(map.get(Rank.CLASS))
            .ifPresent(v -> this.addJsonField("gbifClassKey", v));
        Optional.ofNullable(map.get(Rank.ORDER))
            .ifPresent(v -> this.addJsonField("gbifOrderKey", v));
        Optional.ofNullable(map.get(Rank.FAMILY))
            .ifPresent(v -> this.addJsonField("gbifFamilyKey", v));
        Optional.ofNullable(map.get(Rank.GENUS))
            .ifPresent(v -> this.addJsonField("gbifGenusKey", v));
        Optional.ofNullable(map.get(Rank.SUBGENUS))
            .ifPresent(v -> this.addJsonField("gbifSubgenusKey", v));
        Optional.ofNullable(map.get(Rank.SPECIES))
            .ifPresent(v -> this.addJsonField("gbifSpeciesKey", v));
      }

      // Other Gbif fields
      Optional.ofNullable(taxon.getUsage()).ifPresent(usage ->
              this.addJsonField("gbifTaxonKey", usage.getKey().toString())
                      .addJsonField("gbifScientificName", usage.getName())
                      .addJsonField("gbifTaxonRank", usage.getRank().name())
      );
      // Fields as a common view - "key": "value"
      this.addCommonFields(record);
    };
  }
}
