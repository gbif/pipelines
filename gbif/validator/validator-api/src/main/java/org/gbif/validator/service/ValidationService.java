package org.gbif.validator.service;

import io.vavr.control.Either;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;

public interface ValidationService<MF> {

  Optional<Validation.Error> reachedMaxRunningValidation(String userName);

  Either<Validation.Error, Validation> submitFile(MF file, Principal principal);

  Either<Validation.Error, Validation> validateFileFromUrl(String fileURL, Principal principal);

  Either<Validation.Error, Validation> get(UUID key);

  Either<Validation.Error, Validation> update(Validation validation, Principal principal);

  Either<Validation.Error, Validation> cancel(UUID key, Principal principal);

  PagingResponse<Validation> list(
      Pageable page, Set<Validation.Status> status, Principal principal);
}
