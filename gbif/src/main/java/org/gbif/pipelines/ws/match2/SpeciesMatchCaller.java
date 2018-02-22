package org.gbif.pipelines.ws.match2;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.taxonomy.TaxonomyInterpretationException;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.ws.WsResponse;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import static org.gbif.pipelines.interpretation.taxonomy.TaxonomyUtils.emptyNameUsageMatchResponse;

/**
 * Handles the calls to the species match WS.
 */
public class SpeciesMatchCaller {

  private static final Logger LOG = LoggerFactory.getLogger(SpeciesMatchCaller.class);

  private SpeciesMatchCaller() {}

  /**
   * Matches a {@link ExtendedRecord} to an existing specie using the species match 2 WS.
   *
   * @param extendedRecord avro file with the taxonomic data
   *
   * @return {@link NameUsageMatch2} with the match received from the WS.
   *
   * @throws TaxonomyInterpretationException in case of errors
   */
  public static WsResponse<NameUsageMatch2> getMatch(ExtendedRecord extendedRecord) {
    WsResponse<NameUsageMatch2> response = tryNameMatch(extendedRecord.getCoreTerms());

    if (!isSuccessfulMatch(response) && hasIdentifications(extendedRecord)) {
      LOG.info("Retrying match with identification extension");
      // get identifications
      List<Map<CharSequence, CharSequence>> identifications =
        extendedRecord.getExtensions().get(DwcTerm.Identification.qualifiedName());

      // FIXME: use new generic functions to parse the date??
      // sort them by date identified
      // Ask Markus D if this can be moved to the API?
      identifications.sort(Comparator.comparing((Map<CharSequence, CharSequence> map) -> LocalDateTime.parse(map.get(
        DwcTerm.dateIdentified))).reversed());
      for (Map<CharSequence, CharSequence> record : identifications) {
        response = tryNameMatch(record);
        if (isSuccessfulMatch(response)) {
          LOG.info("match with identificationId {} succeed", record.get(DwcTerm.identificationID.name()));
          return response;
        }
      }
    }

    return response;
  }

  private static WsResponse<NameUsageMatch2> tryNameMatch(Map<CharSequence, CharSequence> terms) {
    SpeciesMatch2Service service = SpeciesMatch2ServiceRest.SINGLE.getService();

    Map<String, String> params = NameUsageMatchQueryConverter.convert(terms);

    Call<NameUsageMatch2> call = service.match(params);

    NameUsageMatch2 responseModel = null;

    try {
      Response<NameUsageMatch2> response = call.execute();

      if (!response.isSuccessful()) {
        String errorMessage = "Call to species match name WS failed: " + response.message();
        return WsResponse.<NameUsageMatch2>fail(response.body(),
                                                response.code(),
                                                errorMessage,
                                                WsResponse.WsErrorCode.CALL_FAILED);
      }

      return WsResponse.<NameUsageMatch2>success(response.body());
    } catch (IOException e) {
      LOG.error("Error calling the match species name WS", e);
      String errorMessage = "Error calling the match species name WS";
      return WsResponse.<NameUsageMatch2>fail(null, errorMessage, WsResponse.WsErrorCode.UNEXPECTED_ERROR);
    }

  }

  private static boolean isSuccessfulMatch(WsResponse<NameUsageMatch2> response) {
    return !response.isResponsyEmpty(emptyNameUsageMatchResponse())
           && !NameUsageMatch.MatchType.NONE.equals(response.getBody().getDiagnostics().getMatchType());
  }

  private static boolean hasIdentifications(ExtendedRecord extendedRecord) {
    return extendedRecord.getExtensions().containsKey(DwcTerm.Identification.qualifiedName());
  }

}
