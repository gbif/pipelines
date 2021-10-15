package org.gbif.validator.ws.security;

import java.io.IOException;
import java.util.UUID;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.registry.security.LegacyAuthorizationService;
import org.gbif.ws.WebApplicationException;
import org.gbif.ws.security.LegacyRequestAuthorization;
import org.gbif.ws.util.CommonWsUtils;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@AllArgsConstructor
@Slf4j
/** Authentication filter for request sent from technical installations like IPT. */
public class InstallationIdentityFilter extends OncePerRequestFilter {

  private final LegacyAuthorizationService legacyAuthorizationService;

  private static final String INSTALLATION_KEY_PARAM = "installationKey";

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws ServletException, IOException {
    UUID installationKey = getInstallationKeyParam(request);
    if (request.getUserPrincipal() == null && installationKey != null) {
      LegacyRequestAuthorization authorization = legacyAuthorizationService.authenticate(request);
      if (legacyAuthorizationService.isAuthorizedToModifyInstallation(
          authorization, installationKey)) {
        SecurityContextHolder.getContext().setAuthentication(authorization);
      } else {
        log.error("Request to register not authorized!");
        throw new WebApplicationException(
            "Request to register not authorized", HttpStatus.UNAUTHORIZED);
      }
    }
    filterChain.doFilter(request, response);
  }

  /** Gets the Organization Key from the request parameters. */
  private static UUID getInstallationKeyParam(HttpServletRequest httpRequest) {
    String uuidParam =
        CommonWsUtils.getFirst(httpRequest.getParameterMap(), INSTALLATION_KEY_PARAM);
    return uuidParam != null ? UUID.fromString(uuidParam) : null;
  }
}
