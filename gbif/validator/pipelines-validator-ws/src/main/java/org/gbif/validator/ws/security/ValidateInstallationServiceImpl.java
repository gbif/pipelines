package org.gbif.validator.ws.security;

import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.gbif.api.model.registry.Installation;
import org.gbif.registry.domain.ws.util.LegacyResourceConstants;
import org.gbif.registry.persistence.mapper.InstallationMapper;
import org.gbif.ws.util.CommonWsUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@RequiredArgsConstructor
public class ValidateInstallationServiceImpl implements ValidateInstallationService {

  private final InstallationMapper installationMapper;

  /** Installation get by key, throws a BAD_REQUEST if the installation can't be found. */
  private Installation tryGetInstallation(UUID installationKey) {
    Installation installation = installationMapper.get(installationKey);
    if (installation == null) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Installation not found " + installationKey);
    }
    return installation;
  }

  /** Gets the Organization Key from the request parameters. */
  private UUID getOrganizationKey(HttpServletRequest httpRequest) {
    String organizationKeyStr =
        CommonWsUtils.getFirst(
            httpRequest.getParameterMap(), LegacyResourceConstants.ORGANIZATION_KEY_PARAM);
    return organizationKeyStr != null ? UUID.fromString(organizationKeyStr) : null;
  }

  /** Validates if the authenticated user can use the installation key in a validation. */
  @Override
  public void validateInstallationAccess(UUID installationKey, HttpServletRequest httpRequest) {
    if (installationKey != null) {
      Installation installation = tryGetInstallation(installationKey);
      UUID organizationKey = getOrganizationKey(httpRequest);
      if (organizationKey != null) {
        if (!organizationKey.equals(installation.getOrganizationKey())) {
          throw new ResponseStatusException(
              HttpStatus.FORBIDDEN, "Installation hosting resource doesn't match installationKey");
        }
      } else {
        throw new ResponseStatusException(
            HttpStatus.FORBIDDEN,
            "User must be authenticate using Organization/Installation password");
      }
    }
  }
}
