package org.gbif.validator.service;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.gbif.validator.api.Validation.ErrorCode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

/** Maps Error responses to ResponseStatusException. */
@Component
@RequiredArgsConstructor
public class ErrorMapper implements Function<ErrorCode, ResponseStatusException> {

  @Value("${maxRunningValidationPerUser}")
  private final int maxRunningValidationPerUser;

  @Override
  public ResponseStatusException apply(ErrorCode error) {
    HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
    String message = "Error processing request";

    switch (error) {
      case MAX_RUNNING_VALIDATIONS:
        status = HttpStatus.METHOD_FAILURE;
        message =
            "Maximum number of executing validations of "
                + maxRunningValidationPerUser
                + " reached\n";
        break;
      case MAX_FILE_SIZE_VIOLATION:
        status = HttpStatus.BAD_REQUEST;
        message = "File exceeds the maximum size allowed";
        break;
      case VALIDATION_IS_NOT_EXECUTING:
        status = HttpStatus.BAD_REQUEST;
        message = "Validation is not in executing state";
        break;
      case NOTIFICATION_EMAILS_MISSING:
        status = HttpStatus.BAD_REQUEST;
        message = "Notification emails must be provided when installation key is present";
        break;
      case WRONG_KEY_IN_REQUEST:
        status = HttpStatus.BAD_REQUEST;
        message = "Wrong validation key for this url";
        break;
      default:
        break;
    }
    return new ResponseStatusException(status, message);
  }
}
