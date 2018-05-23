package org.gbif.pipelines.core.ws.client.match2;

import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonomyValidator;
import org.gbif.pipelines.core.parsers.temporal.ParsedTemporalDates;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.core.ws.HttpConfigFactory;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.client.BaseServiceClient;
import org.gbif.pipelines.core.ws.config.Service;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;

/**
 * Handles the calls to the species match WS.
 */
public class SpeciesMatchv2Client extends BaseServiceClient<NameUsageMatch2, NameUsageMatch2> {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesMatchv2Client.class);

  private final SpeciesMatchv2ServiceRest speciesMatchv2ServiceRest;

  private SpeciesMatchv2Client() {
    speciesMatchv2ServiceRest = SpeciesMatchv2ServiceRest.getInstance();
  }

  private SpeciesMatchv2Client(String wsPropertiesPath) {
    speciesMatchv2ServiceRest =
      SpeciesMatchv2ServiceRest.getInstance(HttpConfigFactory.createConfig(Service.SPECIES_MATCH2,
                                                                           Paths.get(wsPropertiesPath)));
  }

  /**
   * It creates an instance of {@link SpeciesMatchv2Client} reading the ws configuration from a 'ws.properties' file
   * present in the classpath.
   */
  public static SpeciesMatchv2Client newInstance() {
    return new SpeciesMatchv2Client();
  }

  /**
   * It creates an instance of {@link SpeciesMatchv2Client} reading the ws configuration from the path received.
   */
  public static SpeciesMatchv2Client newInstance(String wsPropertiesPath) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(wsPropertiesPath), "ws properties path is required");
    return new SpeciesMatchv2Client(wsPropertiesPath);
  }

  /**
   * Matches a {@link ExtendedRecord} to an existing specie using the species match 2 WS.
   *
   * @param extendedRecord avro file with the taxonomic data
   *
   * @return {@link NameUsageMatch2} with the match received from the WS.
   */
  public HttpResponse<NameUsageMatch2> getMatch(ExtendedRecord extendedRecord) {
    HttpResponse<NameUsageMatch2> response = tryNameMatch(extendedRecord.getCoreTerms());

    if (isSuccessfulMatch(response) || !hasIdentifications(extendedRecord)) {
      return response;
    }

    LOG.info("Retrying match with identification extension");
    // get identifications
    List<Map<String, String>> identifications =
      extendedRecord.getExtensions().get(DwcTerm.Identification.qualifiedName());

    // sort them by date identified
    // Ask Markus D if this can be moved to the API?
    identifications.sort(Comparator.comparing((Map<String, String> map) -> {
      // parse dateIdentified field
      ParsedTemporalDates parsedDates = TemporalParser.parse(map.get(DwcTerm.dateIdentified.qualifiedName()));
      // TODO: I convert it to date just to compare the Temporal objects. Should we change it??
      return TemporalAccessorUtils.toDate(parsedDates.getFrom().orElse(null));
    }).reversed());

    for (Map<String, String> record : identifications) {
      response = tryNameMatch(record);
      if (isSuccessfulMatch(response)) {
        LOG.info("match with identificationId {} succeed", record.get(DwcTerm.identificationID.name()));
        return response;
      }
    }

    return response;
  }

  private HttpResponse<NameUsageMatch2> tryNameMatch(Map<String, String> terms) {
    Map<String, String> params = NameUsageMatchQueryConverter.convert(terms);

    return performCall(params);
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

  private static boolean isSuccessfulMatch(HttpResponse<NameUsageMatch2> response) {
    return !TaxonomyValidator.isEmpty(response.getBody()) && MatchType.NONE != response.getBody()
      .getDiagnostics()
      .getMatchType();
  }

  private static boolean hasIdentifications(ExtendedRecord extendedRecord) {
    return extendedRecord.getExtensions() != null && extendedRecord.getExtensions()
      .containsKey(DwcTerm.Identification.qualifiedName());
  }

}
