package org.gbif.pipelines.crawler.metrics;

import java.io.File;
import java.util.Set;
import java.util.UUID;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.ws.client.ValidationWsClient;

import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor(staticName = "create")
public class ValidationWsClientStub implements ValidationWsClient {

  private Validation validation = Validation.builder().key(UUID.randomUUID()).build();

  @Override
  public Validation submitFile(File file) {
    return validation;
  }

  @Override
  public Validation submitFile(File file, ValidationRequest validationRequest) {
    return validation;
  }

  @Override
  public Validation submitUrl(String fileUrl, ValidationRequest validationRequest) {
    return validation;
  }

  @Override
  public Validation submitUrl(String fileUrl) {
    return validation;
  }

  @Override
  public PagingResponse<Validation> list(Pageable page, Set<Status> statuses) {
    return null;
  }

  @Override
  public Validation get(UUID key) {
    validation.setKey(key);
    return validation;
  }

  @Override
  public void update(UUID key, Validation validation) {
    this.validation = validation;
  }

  @Override
  public void cancel(UUID key) {}
}
