package org.gbif.validator.ws.security;

import java.util.UUID;
import javax.servlet.http.HttpServletRequest;

public interface ValidateInstallationService {

  /** Validates if the authenticated user can use the installation key in a validation. */
  void validateInstallationAccess(UUID installationKey, HttpServletRequest httpRequest);
}
