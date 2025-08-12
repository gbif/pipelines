package org.gbif.pipelines.core.interpreters;

import java.io.Serializable;
import java.util.Arrays;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.NameUsageMatchResponse;

public class NameUsageMatchKeyValueTestStore
    implements KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>, Serializable {

  @Override
  public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
    if (nameUsageMatchRequest.getScientificName().equalsIgnoreCase("None")) {
      return null;
    }
    return createDefaultResponse(nameUsageMatchRequest);
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
}
