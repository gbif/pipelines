package org.gbif.validator.ws.client;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  /** Uploads a file and starts the validation process. */
  default Validation submitFile(@RequestPart("file") File file) {
    return validateFile(file, ValidationRequest.builder().build());
  }

  /** Uploads a file and starts the validation process. */
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  @Override
  Validation validateFile(
      @RequestPart("file") File file, @SpringQueryMap ValidationRequest validationRequest);

  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  @Override
  Validation validateFileFromUrl(
      @RequestPart("fileUrl") String fileUrl, @SpringQueryMap ValidationRequest validationRequest);

  /** Lists the validations of a user. */
  @GetMapping
  PagingResponse<Validation> list(@RequestParam Map<String, Object> validationSearchRequest);

  @Override
  default PagingResponse<Validation> list(ValidationSearchRequest validationSearchRequest) {
    return list(ClientValidationSearchRequest.toQueryMap(validationSearchRequest));
  }

  /** Get a validation data. */
  @GetMapping(path = "/{key}")
  @Override
  Validation get(@PathVariable("key") UUID key);

  /** Updates a validation data. */
  @PutMapping(
      path = "/{key}",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  Validation update(@PathVariable("key") UUID key, @RequestBody Validation validation);

  @Override
  default Validation update(Validation validation) {
    return update(validation.getKey(), validation);
  }

  /** Cancel running validation. */
  @PutMapping(
      path = "/{key}/cancel",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  @Override
  Validation cancel(@PathVariable("key") UUID key);

  /** Cancel running validation. */
  @DeleteMapping(path = "/{key}")
  @Override
  void delete(@PathVariable("key") UUID key);

  /** Get EML as a json. */
  @GetMapping(
      path = "/{key}/eml",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  @Override
  Dataset getDataset(@PathVariable("key") UUID key);

  /** Get running validations uuids */
  @GetMapping(
      path = "/running",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  @Override
  List<UUID> getRunningValidations(@RequestParam("min") int min);

  /** Default factory method for the ValidationWsClient. */
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
