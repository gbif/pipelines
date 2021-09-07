package org.gbif.validator.ws.config;

import com.google.common.collect.Sets;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.ws.util.CommonWsUtils;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

/** Translates a web request into a ValidationRequest object. */
public class ValidationRequestHandlerMethodArgumentResolver
    implements HandlerMethodArgumentResolver {

  @Override
  public boolean supportsParameter(MethodParameter parameter) {
    return parameter.getParameterType().equals(ValidationRequest.class);
  }

  @Override
  public Object resolveArgument(
      MethodParameter parameter,
      ModelAndViewContainer mavContainer,
      NativeWebRequest webRequest,
      WebDataBinderFactory binderFactory)
      throws Exception {
    return getValidationRequest(webRequest);
  }

  protected ValidationRequest getValidationRequest(WebRequest webRequest) {
    ValidationRequest.ValidationRequestBuilder builder = ValidationRequest.builder();
    getUUIDParam(webRequest, "installationKey").ifPresent(builder::installationKey);
    getParam(webRequest, "sourceId").ifPresent(builder::sourceId);
    getSetParam(webRequest, "notificationEmail").ifPresent(builder::notificationEmail);
    return builder.build();
  }

  private Optional<Set<String>> getSetParam(WebRequest webRequest, String paramName) {
    return Optional.ofNullable(webRequest.getParameterValues(paramName)).map(Sets::newHashSet);
  }

  private Optional<String> getParam(WebRequest webRequest, String paramName) {
    return Optional.ofNullable(CommonWsUtils.getFirst(webRequest.getParameterMap(), paramName));
  }

  private Optional<UUID> getUUIDParam(WebRequest webRequest, String paramName) {
    return getParam(webRequest, paramName).map(UUID::fromString);
  }
}
