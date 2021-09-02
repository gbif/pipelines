package org.gbif.validator.ws.security;

import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.NetworkEntity;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.NetworkEntityService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.registry.domain.ws.util.LegacyResourceConstants;
import org.gbif.ws.util.CommonWsUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

@RequiredArgsConstructor
public class ValidateInstallationServiceImpl implements ValidateInstallationService {

  private final InstallationService installationService;
  private final OrganizationService organizationService;

  /** Installation get by key, throws a BAD_REQUEST if the installation can't be found. */
  private Installation tryGetInstallation(UUID installationKey) {
    return tryGetEntity(installationKey, installationService, "Installation");
  }

  /** Installation get by key, throws a BAD_REQUEST if the installation can't be found. */
  private Organization tryGetOrganization(UUID organizationKey) {
    return tryGetEntity(organizationKey, organizationService, "Organization");
  }

  /** Installation get by key, throws a BAD_REQUEST if the installation can't be found. */
  private <T extends NetworkEntity> T tryGetEntity(
      UUID entityKey, NetworkEntityService<T> service, String entityName) {
    T entity = service.get(entityKey);
    if (entity == null) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, entityName + " not found " + entityKey);
    }
    return entity;
  }

  /** Gets the Organization Key from the request parameters. */
  private UUID getOrganizationKey(HttpServletRequest httpRequest) {
    return getUUIDRequestParam(httpRequest, LegacyResourceConstants.ORGANIZATION_KEY_PARAM);
  }

  private UUID getUUIDRequestParam(HttpServletRequest httpRequest, String param) {
    String uuidParam = CommonWsUtils.getFirst(httpRequest.getParameterMap(), param);
    return uuidParam != null ? UUID.fromString(uuidParam) : null;
  }

  /** Validates if the authenticated user can use the installation key in a validation. */
  @Override
  public void validateInstallationAccess(UUID installationKey, HttpServletRequest httpRequest) {
    if (installationKey != null) {
      Installation installation = tryGetInstallation(installationKey);
      UUID organizationKey = getOrganizationKey(httpRequest);
      if (organizationKey != null) {
        Organization organization = tryGetOrganization(organizationKey);
        if (!organization.getKey().equals(installation.getOrganizationKey())) {
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
