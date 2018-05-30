package org.gbif.pipelines.esindexing.response;

import org.gbif.pipelines.esindexing.utils.JsonUtils;

import java.util.Objects;
import java.util.Set;

import org.elasticsearch.client.Response;

public class ResponseParser {

  public static String parseIndexName(Response response) {
    Objects.requireNonNull(response);

    return JsonUtils.readEntity(response.getEntity()).get("index");
  }

  public static Set<String> parseIndexes(Response response) {
    Objects.requireNonNull(response);

    return JsonUtils.readEntity(response.getEntity()).keySet();
  }

}
