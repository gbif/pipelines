package org.gbif.pipelines.core.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Issues;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.avro.specific.SpecificRecordBase;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

  private static final String[] SKIP_KEYS = {"id", "decimalLatitude", "decimalLongitude", "country"};
  private static final String[] REPLACE_KEYS = {"http://rs.tdwg.org/dwc/terms/", "http://purl.org/dc/terms/"};

  private GbifJsonConverter(SpecificRecordBase[] bases) {
    this.setSpecificRecordBase(bases)
        .setSkipKeys(SKIP_KEYS)
        .setReplaceKeys(REPLACE_KEYS)
        .addSpecificConverter(ExtendedRecord.class, getExtendedRecordConverter())
        .addSpecificConverter(LocationRecord.class, getLocationRecordConverter())
        .addSpecificConverter(TemporalRecord.class, getTemporalRecordConverter())
        .addSpecificConverter(TaxonRecord.class, getTaxonomyRecordConverter());
  }

  public static GbifJsonConverter create(SpecificRecordBase... bases) {
    return new GbifJsonConverter(bases);
  }

  /** Change the json result, merging all issues from records to one array */
  @Override
  public ObjectNode buildJson() {
    ObjectNode mainNode = super.buildJson();

    // Issues
    Set<String> issues =
        Arrays.stream(getBases())
            .filter(Issues.class::isInstance)
            .flatMap(x -> ((Issues) x).getIssues().getIssueList().stream())
            .collect(Collectors.toSet());

    ArrayNode arrayIssueNode = MAPPER.createArrayNode();
    issues.forEach(arrayIssueNode::add);
    mainNode.set("issues", arrayIssueNode);

    // Not issues
    Set<String> notIssues =
        Arrays.stream(OccurrenceIssue.values())
            .map(Enum::name)
            .filter(x -> !issues.contains(x))
            .collect(Collectors.toSet());

    ArrayNode arrayNotIssuesNode = MAPPER.createArrayNode();
    notIssues.forEach(arrayNotIssuesNode::add);
    mainNode.set("notIssues", arrayNotIssuesNode);

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

      ExtendedRecord er = (ExtendedRecord) record;

      this.addJsonTextFieldNoCheck("id", er.getId());

      Map<String, String> terms = er.getCoreTerms();

      Optional.ofNullable(terms.get(DwcTerm.recordedBy.qualifiedName()))
          .ifPresent(x -> this.addJsonTextFieldNoCheck("recordedBy", x));
      Optional.ofNullable(terms.get(DwcTerm.organismID.qualifiedName()))
          .ifPresent(x -> this.addJsonTextFieldNoCheck("organismId", x));

      this.addJsonRawObject("verbatim", terms);
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
        this.addJsonObject("coordinates", node);
      }
      // Fields as a common view - "key": "value"
      this.addCommonFields(record);
    };
  }

  /**
   * String converter for {@link TemporalRecord}, convert an object to specific string view
   *
   * <pre>{@code
   * Result example:
   *
   * "startDate": "10/10/2010",
   *  //.....more fields
   *
   * }</pre>
   */
  private Consumer<SpecificRecordBase> getTemporalRecordConverter() {
    return record -> {
      TemporalRecord temporal = (TemporalRecord) record;

      Optional.ofNullable(temporal.getEventDate())
          .map(EventDate::getGte)
          .ifPresent(x -> this.addJsonTextFieldNoCheck("startDate", x));

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
        List<ObjectNode> nodes = new ArrayList<>(classifications.size());
        for (int i = 0; i < classifications.size(); i++) {
          RankedName name = classifications.get(i);
          ObjectNode node = MAPPER.createObjectNode();
          node.put("taxonKey", name.getKey());
          node.put("name", name.getName());
          node.put("depthKey_" + i, name.getKey());
          node.put("kingdomKey", name.getKey());
          node.put("rank", name.getRank().name());
          nodes.add(node);
        }
        this.addJsonArray("backbone", nodes);
      }

      // Other Gbif fields
      Optional.ofNullable(taxon.getUsage())
          .ifPresent(
              usage ->
                  this.addJsonTextFieldNoCheck("gbifTaxonKey", usage.getKey().toString())
                      .addJsonTextFieldNoCheck("gbifScientificName", usage.getName())
                      .addJsonTextFieldNoCheck("gbifTaxonRank", usage.getRank().name()));
    };
  }
}
