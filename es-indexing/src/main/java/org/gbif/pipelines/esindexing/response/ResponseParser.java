package org.gbif.pipelines.esindexing.response;

import org.gbif.pipelines.esindexing.common.JsonUtils;

import java.util.Objects;
import java.util.Set;

import org.elasticsearch.client.Response;

public class ResponseParser {

  public static String parseIndexName(Response response) {
    Objects.requireNonNull(response);

    return JsonUtils.readValueFromEntity(response.getEntity()).get("index");
  }

  public static Set<String> parseIndexes(Response response) {
    Objects.requireNonNull(response);

    return JsonUtils.readValueFromEntity(response.getEntity()).keySet();
  }

}
