package org.gbif.validator.service;

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
public class ErrorMapper implements Function<Validation.ErrorCode, ResponseStatusException> {

  @Value("${maxRunningValidationPerUser}")
  private final int maxRunningValidationPerUser;

  @Override
  public ResponseStatusException apply(Validation.ErrorCode error) {
    if (Validation.ErrorCode.MAX_RUNNING_VALIDATIONS == error) {
      return new ResponseStatusException(
          HttpStatus.METHOD_FAILURE,
          "Maximum number of executing validations of "
              + maxRunningValidationPerUser
              + " reached\n");
    } else if (Validation.ErrorCode.MAX_FILE_SIZE_VIOLATION == error) {
      return new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "File exceeds the maximum size allowed");
    } else if (Validation.ErrorCode.VALIDATION_IS_NOT_EXECUTING == error) {
      return new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Validation is not in executing state");
    } else if (Validation.ErrorCode.NOTIFICATION_EMAILS_MISSING == error) {
      return new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "Notification emails must be provided when installation key is present");
    } else if (Validation.ErrorCode.WRONG_KEY_IN_REQUEST == error) {
      return new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Wrong validation key for this url");
    }
    return new ResponseStatusException(
        HttpStatus.INTERNAL_SERVER_ERROR, "Error processing request");
  }
}
