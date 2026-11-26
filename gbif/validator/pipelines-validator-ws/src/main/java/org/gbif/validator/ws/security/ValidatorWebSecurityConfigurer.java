package org.gbif.validator.ws.security;

import static org.gbif.ws.security.UserRoles.ADMIN_ROLE;

import jakarta.servlet.http.HttpServletResponse;
import java.util.Arrays;
import org.gbif.ws.remoteauth.RemoteAuthWebSecurityConfigurer;
import org.gbif.ws.remoteauth.app.GbifAppRequestFilter;
import org.gbif.ws.remoteauth.basic.BasicAuthRequestFilter;
import org.gbif.ws.remoteauth.jwt.JwtRequestFilter;
import org.gbif.ws.security.GbifAuthUtils;
import org.gbif.ws.security.SecurityUtils;
import org.gbif.ws.server.filter.HttpServletRequestWrapperFilter;
import org.gbif.ws.server.filter.RequestHeaderParamUpdateFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

/**
 * gbif-common-ws remote authentication, denying requests unless authenticated or to a public path.
 */
public class ValidatorWebSecurityConfigurer extends RemoteAuthWebSecurityConfigurer {

  /** Paths that can be used without authentication, everything else requires it. */
  static final String[] PUBLIC_PATHS = {
    "/",
    "/error",
    "/swagger-ui.html",
    "/swagger-ui/**",
    "/v3/api-docs/**",
    "/validation/eml",
    "/validation/eml/schemas",
    "/validation/sequence",
    "/validation/enumeration",
    "/validation/enumeration/**",
    "/actuator/health",
    "/actuator/health/**"
  };

  private static final PathPatternRequestMatcher.Builder PATHS =
      PathPatternRequestMatcher.withDefaults();

  /** ChecklistBank callbacks, authenticated by their token. */
  static final String CLB_CALLBACK_PATH = "/validation/clbValidationCallback/**";

  /**
   * Same as the gbif-common-ws chain, but requests are denied unless authenticated or to a public
   * path, and actuator endpoints other than health are for admins only.
   */
  @Bean
  @Override
  public SecurityFilterChain filterChain(
      HttpSecurity http,
      AuthenticationManager authenticationManager,
      HttpServletRequestWrapperFilter httpServletRequestWrapperFilter,
      RequestHeaderParamUpdateFilter requestHeaderParamUpdateFilter,
      @Value(
              "${gbif.ws.security.gbifAuth.proxyPathPrefixes:"
                  + GbifAuthUtils.DEFAULT_PROXY_PATH_PREFIX
                  + "}")
          String[] proxyPathPrefixes)
      throws Exception {
    return SecurityUtils.gbifFilterChain(
            http, httpServletRequestWrapperFilter, requestHeaderParamUpdateFilter)
        .authorizeHttpRequests(
            auth ->
                auth.requestMatchers(
                        Arrays.stream(PUBLIC_PATHS)
                            .map(PATHS::matcher)
                            .toArray(RequestMatcher[]::new))
                    .permitAll()
                    .requestMatchers(PATHS.matcher(HttpMethod.POST, CLB_CALLBACK_PATH))
                    .permitAll()
                    .requestMatchers(PATHS.matcher("/actuator/**"))
                    .hasAuthority(ADMIN_ROLE)
                    .anyRequest()
                    .authenticated())
        .exceptionHandling(
            e ->
                e.authenticationEntryPoint(
                    (request, response, ex) -> {
                      response.setHeader("WWW-Authenticate", "GBIF realm=\"GBIF\"");
                      response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
                    }))
        .addFilterAfter(
            new BasicAuthRequestFilter(authenticationManager), RequestHeaderParamUpdateFilter.class)
        .addFilterAfter(new JwtRequestFilter(authenticationManager), BasicAuthRequestFilter.class)
        .addFilterAfter(
            new GbifAppRequestFilter(authenticationManager, Arrays.asList(proxyPathPrefixes)),
            JwtRequestFilter.class)
        .build();
  }
}
