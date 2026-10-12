package org.gbif.validator.ws.client;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.service.ValidationService;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.cloud.openfeign.SpringQueryMap;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;

@RequestMapping(value = "/validation", produces = MediaType.APPLICATION_JSON_VALUE)
public interface ValidationWsClient extends ValidationService<File> {

  default Validation submitFile(@RequestPart("file") File file) {
    return validateFile(file, ValidationRequest.builder().build());
  }

  @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @Override
  Validation validateFile(
      @RequestPart("file") File file, @SpringQueryMap ValidationRequest validationRequest);

  @PostMapping(path = "/url", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @Override
  Validation validateFileFromUrl(
      @RequestPart("fileUrl") String fileUrl, @SpringQueryMap ValidationRequest validationRequest);

  @GetMapping
  PagingResponse<Validation> list(@RequestParam Map<String, Object> validationSearchRequest);

  @Override
  default PagingResponse<Validation> list(ValidationSearchRequest validationSearchRequest) {
    return list(ClientValidationSearchRequest.toQueryMap(validationSearchRequest));
  }

  @GetMapping(path = "/{key}")
  @Override
  Validation get(@PathVariable("key") UUID key);

  @PutMapping(path = "/{key}", consumes = MediaType.APPLICATION_JSON_VALUE)
  Validation update(@PathVariable("key") UUID key, @RequestBody Validation validation);

  @Override
  default Validation update(Validation validation) {
    return update(validation.getKey(), validation);
  }

  @PutMapping(path = "/{key}/cancel", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Override
  Validation cancel(@PathVariable("key") UUID key);

  @DeleteMapping(path = "/{key}")
  @Override
  void delete(@PathVariable("key") UUID key);

  @GetMapping(path = "/{key}/eml", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Override
  Dataset getDataset(@PathVariable("key") UUID key);

  @GetMapping(path = "/running", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Override
  List<UUID> getRunningValidations(@RequestParam("min") int min);

  /** Upserts a typed report for this validation run. The body is raw JSON, not JSON-quoted text. */
  @PutMapping(
      path = "/{key}/validationreport/{attempt}/{reportType}",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  void createOrUpdateValidationReport(
      @PathVariable("key") UUID key,
      @PathVariable("attempt") int attempt,
      @PathVariable("reportType") String reportType,
      @RequestBody String report);

  @GetMapping(path = "/{key}/validationreport/{attempt}/{reportType}")
  String getValidationReport(
      @PathVariable("key") UUID key,
      @PathVariable("attempt") int attempt,
      @PathVariable("reportType") String reportType);

  static ValidationWsClient getInstance(String url, String userName, String password) {
    return new ClientBuilder()
        .withUrl(url)
        .withCredentials(userName, password)
        .withFormEncoder()
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
        .build(ValidationWsClient.class);
  }
}
