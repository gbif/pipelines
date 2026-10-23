package org.gbif.pipelines.backbone.impact;

import feign.FeignException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchingService;

/**
 * Looks up the verbatim classification of each source row against the name matching service,
 * bypassing the key value cache, and emits a report row for each classification that differs from
 * the current one.
 */
@Slf4j
public class ImpactMatcher implements MapPartitionsFunction<Row, Row> {

  private static final String[] VERBATIM_HEADERS = {
    "verbatim_taxonID",
    "verbatim_taxonConceptID",
    "verbatim_scientificNameID",
    "verbatim_kingdom",
    "verbatim_phylum",
    "verbatim_class",
    "verbatim_order",
    "verbatim_superfamily",
    "verbatim_family",
    "verbatim_subfamily",
    "verbatim_tribe",
    "verbatim_subtribe",
    "verbatim_genus",
    "verbatim_specificEpithet",
    "verbatim_infraspecificEpithet",
    "verbatim_rank",
    "verbatim_verbatimRank",
    "verbatim_scientificName",
    "verbatim_genericName",
    "verbatim_author"
  };

  private final String baseAPIUrl;
  private final String checklistKey;
  private final boolean skipKeys;
  private final boolean ignoreWhitespace;
  private final boolean ignoreAuthorshipFormatting;
  private final boolean ignoreAuthorship;
  private final MatchServiceFactory serviceFactory;

  public ImpactMatcher(GbifImpactArgs args, MatchServiceFactory serviceFactory) {
    this.baseAPIUrl = args.apiBaseUri;
    this.checklistKey = args.checklistKey;
    this.skipKeys = args.skipKeys;
    this.ignoreWhitespace = args.ignoreWhitespace;
    this.ignoreAuthorshipFormatting = args.ignoreAuthorshipFormatting;
    this.ignoreAuthorship = args.ignoreAuthorship;
    this.serviceFactory = serviceFactory;
  }

  /** The column names of the report, in order. */
  public static String[] headers(boolean skipKeys) {
    return Stream.of(
            Stream.of("count"),
            Arrays.stream(VERBATIM_HEADERS),
            Arrays.stream(Classification.headers("current_", skipKeys)),
            Arrays.stream(Classification.headers("proposed_", skipKeys)),
            Stream.of("debug_url"))
        .flatMap(s -> s)
        .toArray(String[]::new);
  }

  /** The schema of the report rows: the count followed by string columns. */
  public static StructType schema(boolean skipKeys) {
    String[] headers = headers(skipKeys);
    StructField[] fields = new StructField[headers.length];
    fields[0] = DataTypes.createStructField(headers[0], DataTypes.LongType, false);
    for (int i = 1; i < headers.length; i++) {
      fields[i] = DataTypes.createStructField(headers[i], DataTypes.StringType, true);
    }
    return DataTypes.createStructType(fields);
  }

  @Override
  public Iterator<Row> call(Iterator<Row> rows) {
    NameUsageMatchingService service = serviceFactory.create();
    Iterable<Row> source = () -> rows;
    return StreamSupport.stream(source.spliterator(), false)
        .map(row -> match(service, row))
        .filter(Objects::nonNull)
        .iterator();
  }

  /** Returns the report row, or null if the classification has not changed. */
  Row match(NameUsageMatchingService service, Row source) {
    // We use the request to ensure we apply the same "clean" operations as the production
    // pipelines, even though we short circuit the cache and use the lookup service directly.
    NameUsageMatchRequest matchRequest =
        NameUsageMatchRequest.builder()
            .withKingdom(source.getAs("v_kingdom"))
            .withPhylum(source.getAs("v_phylum"))
            .withClazz(source.getAs("v_class"))
            .withOrder(source.getAs("v_order"))
            .withSuperfamily(source.getAs("v_superfamily"))
            .withFamily(source.getAs("v_family"))
            .withSubfamily(source.getAs("v_subfamily"))
            .withTribe(source.getAs("v_tribe"))
            .withSubtribe(source.getAs("v_subtribe"))
            .withGenus(source.getAs("v_genus"))
            .withScientificName(source.getAs("v_scientificname"))
            .withScientificNameAuthorship(source.getAs("v_scientificnameauthorship"))
            .withGenericName(source.getAs("v_genericname"))
            .withSpecificEpithet(source.getAs("v_specificepithet"))
            .withInfraspecificEpithet(source.getAs("v_infraspecificepithet"))
            .withTaxonRank(source.getAs("v_taxonrank"))
            .withVerbatimTaxonRank(source.getAs("v_verbatimtaxonrank"))
            .withScientificNameID(source.getAs("v_scientificnameid"))
            .withTaxonID(source.getAs("v_taxonid"))
            .withTaxonConceptID(source.getAs("v_taxonconceptid"))
            .withChecklistKey(checklistKey)
            .build();

    try {
      // short circuit the cache, but replicate same logic of the NameUsageMatchKVStoreFactory
      NameUsageMatchResponse usageMatch = null;
      try {
        usageMatch = service.match(matchRequest);
      } catch (FeignException.InternalServerError e) {
        log.warn("Caught 500 Internal Server Error: {}", e.getMessage());
      }

      Classification existing = Classification.buildFromRow(source);
      Classification proposed;
      if (usageMatch == null) {
        proposed = Classification.error();
      } else if (isEmpty(usageMatch)) {
        proposed = Classification.newIncertaeSedis();
      } else {
        proposed = Classification.buildFromNameUsageMatch(usageMatch);
      }

      // copy pipelines to put unknown content into incertae sedis kingdom
      if (proposed.getKingdom() == null) {
        log.warn("Failed request: {}", toDebugUrl(baseAPIUrl, matchRequest));
        proposed.setKingdom("incertae sedis");
        proposed.setKingdomKey("0");
      }

      // emit classifications that differ, optionally considering the keys and authorship
      Classification existingCmp = existing;
      Classification proposedCmp = proposed;
      if (ignoreAuthorship) {
        existingCmp = existing.withoutAuthorship(AuthorshipStripper::strip);
        proposedCmp = proposed.withoutAuthorship(AuthorshipStripper::strip);
      }
      boolean changed =
          skipKeys
              ? !existingCmp.classificationEquals(
                  proposedCmp, ignoreWhitespace, ignoreAuthorshipFormatting)
              : !existingCmp.equals(proposedCmp);

      return changed
          ? toRow(source.getAs("occurrencecount"), matchRequest, existing, proposed)
          : null;

    } catch (Exception e) {
      log.error("Lookup failed: {}", matchRequest, e);
      throw e; // fail the job
    }
  }

  /** Formats the data for the output row in the report. */
  private Row toRow(
      long count, NameUsageMatchRequest verbatim, Classification current, Classification proposed) {

    List<Object> values = new ArrayList<>();
    values.add(count);
    values.add(verbatim.getTaxonID());
    values.add(verbatim.getTaxonConceptID());
    values.add(verbatim.getScientificNameID());
    values.add(verbatim.getKingdom());
    values.add(verbatim.getPhylum());
    values.add(verbatim.getClazz());
    values.add(verbatim.getOrder());
    values.add(verbatim.getSuperfamily());
    values.add(verbatim.getFamily());
    values.add(verbatim.getSubfamily());
    values.add(verbatim.getTribe());
    values.add(verbatim.getSubtribe());
    values.add(verbatim.getGenus());
    values.add(verbatim.getSpecificEpithet());
    values.add(verbatim.getInfraspecificEpithet());
    values.add(verbatim.getTaxonRank());
    values.add(verbatim.getTaxonRank()); // avoid breaking the API (verbatimTaxonRank)
    values.add(verbatim.getScientificName());
    values.add(verbatim.getGenericName());
    values.add(verbatim.getScientificNameAuthorship());
    values.addAll(current.values(skipKeys));
    values.addAll(proposed.values(skipKeys));
    values.add(toDebugUrl(baseAPIUrl, verbatim));
    return RowFactory.create(values.toArray());
  }

  public static String toDebugUrl(String apiUrl, NameUsageMatchRequest matchRequest) {
    List<String> params = new ArrayList<>();
    addParam(params, "taxonID", matchRequest.getTaxonID());
    addParam(params, "taxonConceptID", matchRequest.getTaxonConceptID());
    addParam(params, "scientificNameID", matchRequest.getScientificNameID());
    addParam(params, "kingdom", matchRequest.getKingdom());
    addParam(params, "phylum", matchRequest.getPhylum());
    addParam(params, "class", matchRequest.getClazz());
    addParam(params, "order", matchRequest.getOrder());
    addParam(params, "family", matchRequest.getFamily());
    addParam(params, "genus", matchRequest.getGenus());
    addParam(params, "scientificName", matchRequest.getScientificName());
    addParam(params, "rank", matchRequest.getTaxonRank());
    addParam(params, "checklistKey", matchRequest.getChecklistKey());
    params.add("verbose=true");
    return apiUrl + "/v2/species/match?" + String.join("&", params);
  }

  private static void addParam(List<String> params, String name, String value) {
    if (value != null) {
      params.add(name + "=" + URLEncoder.encode(value, StandardCharsets.UTF_8));
    }
  }

  private static boolean isEmpty(NameUsageMatchResponse response) {
    return response.getUsage() == null
        || (response.getClassification() == null || response.getClassification().isEmpty());
  }
}
