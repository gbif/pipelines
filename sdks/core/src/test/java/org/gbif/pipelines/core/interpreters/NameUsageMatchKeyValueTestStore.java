package org.gbif.pipelines.core.interpreters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.species.NameUsageMatchResponse;

public class NameUsageMatchKeyValueTestStore
    implements KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse>, Serializable {

  private final Map<NameUsageMatchRequest, NameUsageMatchResponse> map = new HashMap<>();

  @Override
  public NameUsageMatchResponse get(NameUsageMatchRequest nameUsageMatchRequest) {
    NameUsageMatchResponse response = new NameUsageMatchResponse();
    Usage.
    response.setUsage();

    return response;
  }

  @Override
  public void close() {}
}
