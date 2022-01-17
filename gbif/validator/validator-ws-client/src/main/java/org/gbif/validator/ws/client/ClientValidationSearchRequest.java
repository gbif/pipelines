package org.gbif.validator.ws.client;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.gbif.validator.api.ValidationSearchRequest;

@UtilityClass
public class ClientValidationSearchRequest {

  public static Map<String, Object> toQueryMap(ValidationSearchRequest searchRequest) {

    Map<String, Object> queryParams = new HashMap<>();
    if (searchRequest.getSourceId() != null) {
      queryParams.put("sourceId", searchRequest.getSourceId());
    }
    if (searchRequest.getInstallationKey() != null) {
      queryParams.put("installationKey", searchRequest.getInstallationKey());
    }
    if (searchRequest.getOffset() != null) {
      queryParams.put("offset", searchRequest.getOffset());
    }
    if (searchRequest.getLimit() != null) {
      queryParams.put("limit", searchRequest.getLimit());
    }
    if (searchRequest.getStatus() != null) {
      queryParams.put("status", searchRequest.getStatus());
    }
    if (searchRequest.getFromDate() != null) {
      queryParams.put(
          "fromDate",
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'").format(searchRequest.getFromDate()));
    }
    if (searchRequest.getToDate() != null) {
      queryParams.put(
          "toDate",
          new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'").format(searchRequest.getToDate()));
    }
    if (searchRequest.getSortBy() != null) {
      queryParams.put(
          "sortBy",
          searchRequest.getSortBy().stream()
              .map(
                  sortBy ->
                      sortBy.getField()
                          + Optional.ofNullable(sortBy.getOrder())
                              .map(o -> ":" + o.name())
                              .orElse(""))
              .collect(Collectors.toSet()));
    }
    return queryParams;
  }
}
