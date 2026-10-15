package org.gbif.validator.api;

import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.HttpExchange;
import org.springframework.web.service.annotation.PutExchange;

@HttpExchange(url = "/validation", accept = MediaType.APPLICATION_JSON_VALUE)
public interface ValidationApi {

  @PutExchange(
      url = "/{key}/validationreport/{attempt}/{reportType}",
      contentType = MediaType.APPLICATION_JSON_VALUE)
  void createOrUpdateValidationReport(
      @PathVariable("key") UUID key,
      @PathVariable("attempt") int attempt,
      @PathVariable("reportType") String reportType,
      @RequestBody String report);

  @GetExchange("/{key}/validationreport/{attempt}/{reportType}")
  String getValidationReport(
      @PathVariable("key") UUID key,
      @PathVariable("attempt") int attempt,
      @PathVariable("reportType") String reportType);
}
