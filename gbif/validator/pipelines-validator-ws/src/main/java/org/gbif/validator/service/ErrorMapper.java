package org.gbif.validator.service;

import io.vavr.control.Either;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.gbif.validator.api.Validation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Maps Error responses to ResponseStatusException. */
@Component
@RequiredArgsConstructor
public class ErrorMapper implements Function<Validation.Error, ResponseStatusException> {

  @Value("${maxRunningValidationPerUser}")
  private final int maxRunningValidationPerUser;

  @Override
  public ResponseStatusException apply(Validation.Error error) {
    if (Validation.Error.Code.MAX_RUNNING_VALIDATIONS == error.getCode()) {
      return new ResponseStatusException(
          HttpStatus.METHOD_FAILURE,
          "Maximum number of executing validations of "
              + maxRunningValidationPerUser
              + " reached\n");
    } else if (Validation.Error.Code.MAX_FILE_SIZE_VIOLATION == error.getCode()) {
      return new ResponseStatusException(HttpStatus.BAD_REQUEST, error.getError().getMessage());
    } else if (Validation.Error.Code.VALIDATION_IS_NOT_EXECUTING == error.getCode()) {
      return new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Validation is not in executing state");
    }
    return new ResponseStatusException(
        HttpStatus.INTERNAL_SERVER_ERROR, "Error processing request");
  }

  /** Gets a result or maps the exception. */
  public <T> T getOrElseThrow(Either<Validation.Error, T> result) {
    return result.getOrElseThrow(this);
  }
}
