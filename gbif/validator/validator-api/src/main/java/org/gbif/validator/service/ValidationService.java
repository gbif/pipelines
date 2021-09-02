package org.gbif.validator.service;

import io.vavr.control.Either;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;

/**
 * Data validation service.
 *
 * @param <MF> multi-part file upload type
 */
public interface ValidationService<MF> {

  Optional<Validation.Error> reachedMaxRunningValidations(String userName);

  Either<Validation.Error, Validation> validateFile(
      MF file,
      Principal principal,
      String sourceId,
      UUID installationKey,
      Set<String> notificationEmails);

  Either<Validation.Error, Validation> validateFileFromUrl(
      String fileURL,
      Principal principal,
      String sourceId,
      UUID installationKey,
      Set<String> notificationEmails);

  Either<Validation.Error, Validation> get(UUID key);

  Either<Validation.Error, Validation> update(Validation validation, Principal principal);

  Either<Validation.Error, Validation> cancel(UUID key, Principal principal);

  PagingResponse<Validation> list(
      Pageable page,
      Set<Validation.Status> status,
      UUID installationKey,
      String sourceId,
      Principal principal);
}
