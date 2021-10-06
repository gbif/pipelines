package org.gbif.validator.ws.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.ws.util.CommonWsUtils;
import org.springframework.web.context.request.WebRequest;

@UtilityClass
public class ValidationRequestParams {

  public static final int DEFAULT_LIMIT = 20;

  public static final String INSTALLATION_KEY = "installationKey";
  public static final String SOURCE_ID = "sourceId";
  public static final String STATUS = "status";
  public static final String SORT_BY = "sortBy";
  public static final String NOTIFICATION_EMAIL = "notificationEmail";
  public static final String CREATED = "created";
  public static final String OFFSET = "offset";
  public static final String LIMIT = "limit";

  private static final String SORT_BY_SEPARATOR = ":";

  private static final Set<String> SORTABLES =
      new HashSet<>(Arrays.asList(INSTALLATION_KEY, CREATED));

  private static Optional<String> sorteable(String fieldName) {
    return SORTABLES.stream().filter(s -> s.equalsIgnoreCase(fieldName)).findFirst();
  }

  public static String sortField(String fieldName) {
    return sorteable(fieldName)
        .orElseThrow(
            () ->
                new IllegalArgumentException(fieldName + " is not a valid field to sort results"));
  }

  public static Optional<UUID> getInstallationKeyParam(WebRequest webRequest) {
    return getUUIDParam(webRequest, INSTALLATION_KEY);
  }

  public static Optional<String> getSourceIdParam(WebRequest webRequest) {
    return getParam(webRequest, SOURCE_ID);
  }

  public static Optional<Set<String>> getNotificationEmailParam(WebRequest webRequest) {
    return getSetParam(webRequest, NOTIFICATION_EMAIL);
  }

  public static Optional<Set<Validation.Status>> getStatusParam(WebRequest webRequest) {
    return getSetParam(webRequest, STATUS, Validation.Status::valueOf);
  }

  public static List<ValidationSearchRequest.SortBy> getSortByParam(WebRequest webRequest) {
    String[] sortByValues = webRequest.getParameterValues(SORT_BY);
    if (sortByValues != null) {
      return Arrays.stream(sortByValues)
          .map(ValidationRequestParams::parseSortBy)
          .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public static Long getOffsetParam(WebRequest webRequest) {
    return getLongParam(webRequest, OFFSET).orElse(0L);
  }

  public static Integer getLimitParam(WebRequest webRequest) {
    return getIntParam(webRequest, LIMIT).orElse(DEFAULT_LIMIT);
  }

  private static ValidationSearchRequest.SortBy parseSortBy(String sortBy) {
    String[] tokens = sortBy.split(SORT_BY_SEPARATOR);
    ValidationSearchRequest.SortBy.SortByBuilder builder = ValidationSearchRequest.SortBy.builder();
    if (tokens.length == 2) {
      return builder
          .field(sortField(tokens[0]))
          .order(ValidationSearchRequest.SortOrder.valueOf(tokens[1].toUpperCase()))
          .build();
    } else if (tokens.length == 1) {
      return builder
          .field(sortField(tokens[0]))
          .order(ValidationSearchRequest.SortOrder.DESC)
          .build();
    }
    throw new IllegalArgumentException("Invalid sortBy parameters: " + sortBy);
  }

  public static Optional<Set<String>> getSetParam(WebRequest webRequest, String paramName) {
    return Optional.ofNullable(webRequest.getParameterValues(paramName))
        .map(x -> new HashSet<>(Arrays.asList(x)));
  }

  public static <T> Optional<Set<T>> getSetParam(
      WebRequest webRequest, String paramName, Function<String, T> mapper) {
    return Optional.ofNullable(webRequest.getParameterValues(paramName))
        .map(values -> Arrays.stream(values).map(mapper).collect(Collectors.toSet()));
  }

  public static Optional<String> getParam(WebRequest webRequest, String paramName) {
    return Optional.ofNullable(CommonWsUtils.getFirst(webRequest.getParameterMap(), paramName));
  }

  public static <T> Optional<T> getParam(
      WebRequest webRequest, String paramName, Function<String, T> mapper) {
    return Optional.ofNullable(CommonWsUtils.getFirst(webRequest.getParameterMap(), paramName))
        .map(mapper);
  }

  public static Optional<Integer> getIntParam(WebRequest webRequest, String paramName) {
    return getParam(webRequest, paramName, Integer::parseInt);
  }

  public static Optional<Long> getLongParam(WebRequest webRequest, String paramName) {
    return getParam(webRequest, paramName, Long::parseLong);
  }

  public static Optional<UUID> getUUIDParam(WebRequest webRequest, String paramName) {
    return getParam(webRequest, paramName, UUID::fromString);
  }

  public ValidationRequest getValidationRequest(WebRequest webRequest) {
    ValidationRequest.ValidationRequestBuilder builder = ValidationRequest.builder();

    getInstallationKeyParam(webRequest).ifPresent(builder::installationKey);
    getSourceIdParam(webRequest).ifPresent(builder::sourceId);
    getNotificationEmailParam(webRequest).ifPresent(builder::notificationEmail);

    return builder.build();
  }

  public ValidationSearchRequest getValidationSearchRequest(WebRequest webRequest) {
    ValidationSearchRequest.ValidationSearchRequestBuilder builder =
        ValidationSearchRequest.builder();

    getInstallationKeyParam(webRequest).ifPresent(builder::installationKey);
    getSourceIdParam(webRequest).ifPresent(builder::sourceId);
    getStatusParam(webRequest).ifPresent(builder::status);
    builder.offset(getOffsetParam(webRequest));
    builder.limit(getLimitParam(webRequest));
    builder.sortBy(getSortByParam(webRequest));

    return builder.build();
  }
}
