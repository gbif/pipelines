package org.gbif.pipelines.parsers.ws.client.match2;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;
import org.gbif.pipelines.parsers.ws.HttpResponse;
import org.gbif.pipelines.parsers.ws.client.BaseServiceClient;
import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

/** Handles the calls to the species match WS. */
public class SpeciesMatchv2Client extends BaseServiceClient<NameUsageMatch2, NameUsageMatch2> {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesMatchv2Client.class);

  private final SpeciesMatchv2ServiceRest speciesMatchv2ServiceRest;

  private SpeciesMatchv2Client(WsConfig wsConfig) {
    speciesMatchv2ServiceRest = SpeciesMatchv2ServiceRest.getInstance(wsConfig);
  }

  /**
   * It creates an instance of {@link SpeciesMatchv2Client} reading the ws configuration from the
   * path received.
   */
  public static SpeciesMatchv2Client create(WsConfig wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new SpeciesMatchv2Client(wsConfig);
  }

  /**
   * Matches a {@link ExtendedRecord} to an existing specie using the species match 2 WS.
   *
   * @param extendedRecord avro file with the taxonomic data
   * @return {@link NameUsageMatch2} with the match received from the WS.
   */
  public HttpResponse<NameUsageMatch2> getMatch(ExtendedRecord extendedRecord) {
    HttpResponse<NameUsageMatch2> response = tryNameMatch(extendedRecord.getCoreTerms());

    if (isSuccessfulMatch(response) || !hasIdentifications(extendedRecord)) {
      return response;
    }

    LOG.info("Retrying match with identification extension");

    return extendedRecord
        .getExtensions()
        .get(DwcTerm.Identification.qualifiedName())
        .stream()
        .sorted(sortByDateIdentified())
        .map(this::tryNameMatch)
        .filter(this::isSuccessfulMatch)
        .findFirst()
        .orElse(response);
  }

  @Override
  protected Call<NameUsageMatch2> getCall(Map<String, String> params) {
    return speciesMatchv2ServiceRest.getService().match(params);
  }

  @Override
  protected String getErrorMessage() {
    return "Call to species match name WS failed";
  }

  @Override
  protected NameUsageMatch2 parseResponse(NameUsageMatch2 body) {
    return body;
  }

  /** Ask Markus D if this can be moved to the API? */
  private Comparator<Map<String, String>> sortByDateIdentified() {

    Function<Map<String, String>, Date> fn =
        map -> {
          ParsedTemporal dates =
              TemporalParser.parse(map.get(DwcTerm.dateIdentified.qualifiedName()));
          // if it's null we return the minimum date to give it the least priority
          return dates.getFrom().map(TemporalAccessorUtils::toDate).orElse(new Date(0L));
        };

    return Comparator.comparing(fn).reversed();
  }

  private HttpResponse<NameUsageMatch2> tryNameMatch(Map<String, String> terms) {
    Map<String, String> params = MatchQueryConverter.convert(terms);

    return performCall(params);
  }

  private boolean isSuccessfulMatch(HttpResponse<NameUsageMatch2> response) {
    NameUsageMatch2 body = response.getBody();

    boolean isEmpty =
        body == null
            || body.getUsage() == null
            || body.getClassification() == null
            || body.getDiagnostics() == null;

    return !isEmpty && MatchType.NONE != body.getDiagnostics().getMatchType();
  }

  private boolean hasIdentifications(ExtendedRecord extendedRecord) {
    return extendedRecord.getExtensions() != null
        && extendedRecord.getExtensions().containsKey(DwcTerm.Identification.qualifiedName());
  }
}
