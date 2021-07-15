package org.gbif.validator.ws.client;

import java.io.File;
import java.util.UUID;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;
import org.springframework.cloud.openfeign.SpringQueryMap;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestPart;

@RequestMapping(value = "/validation", produces = MediaType.APPLICATION_JSON_VALUE)
public interface ValidationWsClient {

  /** Uploads a file and starts the validation process. */
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  Validation submitFile(@RequestPart("file") File file);

  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  Validation submitUrl(@RequestPart("fileUrl") String fileUrl);

  /** Lists the validations of an user. */
  @GetMapping
  PagingResponse<Validation> list(@SpringQueryMap Pageable page);

  /** Get a validation data. */
  @GetMapping(path = "/{key}")
  Validation get(@PathVariable("key") UUID key);

  /** Updates a validation data. */
  @RequestMapping(
      method = RequestMethod.PUT,
      path = "/{key}",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  void update(@PathVariable("key") UUID key, @RequestBody Validation validation);

  default void update(Validation validation) {
    update(validation.getKey(), validation);
  }
}
