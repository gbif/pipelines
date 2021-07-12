package org.gbif.validator.ws.config;

import org.gbif.validator.ws.file.FileSizeException;
import org.gbif.validator.ws.file.UnsupportedMediaTypeException;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/** Spring Exceptions to ResponseEntity mappings. */
@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice
public class ExceptionMappingAdvice extends ResponseEntityExceptionHandler {

  @ExceptionHandler(UnsupportedMediaTypeException.class)
  protected ResponseEntity<Object> unsupportedMediaTypeException(UnsupportedMediaTypeException ex) {
    return ResponseEntity.badRequest().body(ex.getMessage());
  }

  @ExceptionHandler(FileSizeException.class)
  protected ResponseEntity<Object> fileSizeException(FileSizeException ex) {
    return ResponseEntity.badRequest().body(ex.getMessage());
  }
}
