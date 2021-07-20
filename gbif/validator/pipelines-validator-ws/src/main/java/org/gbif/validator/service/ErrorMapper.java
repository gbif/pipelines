package org.gbif.validator.service;

import java.util.function.Function;
import lombok.AllArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Maps Error responses to ResponseStatusException. */
@AllArgsConstructor(staticName = "newInstance")
public class ErrorMapper implements Function<ValidationService.Error, ResponseStatusException> {

  private final int maxRunningValidationPerUser;

  @Override
  public ResponseStatusException apply(ValidationService.Error error) {
    if (ValidationService.Error.Code.MAX_RUNNING_VALIDATIONS == error.getCode()) {
      return new ResponseStatusException(
          HttpStatus.METHOD_FAILURE,
          "Maximum number of executing validations of "
              + maxRunningValidationPerUser
              + " reached\n");
    } else if (ValidationService.Error.Code.MAX_FILE_SIZE_VIOLATION == error.getCode()) {
      return new ResponseStatusException(HttpStatus.BAD_REQUEST, error.getError().getMessage());
    } else if (ValidationService.Error.Code.VALIDATION_IS_NOT_EXECUTING == error.getCode()) {
      return new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Validation is not in executing state");
    }
    return new ResponseStatusException(
        HttpStatus.INTERNAL_SERVER_ERROR, "Error processing request");
  }
}
