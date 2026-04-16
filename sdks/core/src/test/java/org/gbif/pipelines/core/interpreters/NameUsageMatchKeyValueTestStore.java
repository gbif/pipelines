package org.gbif.pipelines.core.interpreters;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.NameUsageMatchResponse;

public class NameUsageMatchKeyValueTestStore
    implements KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>, Serializable {

  public static final String DEFAULT_CHECKLIST_KEY = UUID.randomUUID().toString();
  public static final String ANOTHER_CHECKLIST_KEY = UUID.randomUUID().toString();

  @Override
  public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
    if (nameUsageMatchRequest.getScientificName().equalsIgnoreCase("None")) {
      return null;
    }
    if (nameUsageMatchRequest.getChecklistKey().equals(DEFAULT_CHECKLIST_KEY)) {
      return createDefaultResponse(nameUsageMatchRequest);
    } else {
      return createAnotherChecklistResponse(nameUsageMatchRequest);
    }
  }

  @Override
  public void close() {}

  private NameUsageMatchResponse createDefaultResponse(NameUsageMatchRequest request) {
    NameUsageMatchResponse response = new NameUsageMatchResponse();
    response.setUsage(
        NameUsageMatchResponse.Usage.builder()
            .withName(request.getScientificName())
            .withKey("1")
            .withRank("rank")
            .build());

    response.setClassification(
        Arrays.asList(
            NameUsageMatchResponse.RankedName.builder()
                .withName("rn1")
                .withKey("1")
                .withRank("r1")
                .build(),
            NameUsageMatchResponse.RankedName.builder()
                .withName("rn2")
                .withKey("2")
                .withRank("r2")
                .build()));

    return response;
  }

  private NameUsageMatchResponse createAnotherChecklistResponse(NameUsageMatchRequest request) {
    NameUsageMatchResponse response = new NameUsageMatchResponse();
    response.setUsage(
        NameUsageMatchResponse.Usage.builder()
            .withName(request.getScientificName())
            .withKey("one")
            .withRank("rank")
            .build());

    response.setClassification(
        Arrays.asList(
            NameUsageMatchResponse.RankedName.builder()
                .withName("rankedNameOne")
                .withKey("one")
                .withRank("rankOne")
                .build(),
            NameUsageMatchResponse.RankedName.builder()
                .withName("rankedNameTwo")
                .withKey("two")
                .withRank("rankTwo")
                .build()));

    return response;
  }
}
