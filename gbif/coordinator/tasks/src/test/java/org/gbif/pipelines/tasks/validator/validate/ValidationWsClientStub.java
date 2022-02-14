package org.gbif.pipelines.tasks.validator.validate;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.ws.client.ValidationWsClient;

@Builder
public class ValidationWsClientStub implements ValidationWsClient {

  private final UUID key;
  @Builder.Default private final Validation validation = Validation.builder().build();
  private Status status;

  @Override
  public boolean reachedMaxRunningValidations(String userName) {
    return false;
  }

  @Override
  public Validation submitFile(File file) {
    return null;
  }

  @Override
  public Validation validateFile(File file, ValidationRequest validationRequest) {
    return null;
  }

  @Override
  public Validation validateFileFromUrl(String fileUrl, ValidationRequest validationRequest) {
    return null;
  }

  @Override
  public PagingResponse<Validation> list(Map<String, Object> validationSearchRequest) {
    return null;
  }

  @Override
  public PagingResponse<Validation> list(ValidationSearchRequest searchRequest) {
    return null;
  }

  @Override
  public Validation get(UUID key) {
    return validation;
  }

  @Override
  public Validation update(UUID key, Validation validation) {
    return validation;
  }

  @Override
  public Validation update(Validation validation) {
    return validation;
  }

  @Override
  public Validation cancel(UUID key) {
    this.status = Status.ABORTED;
    return Validation.builder().key(key).status(Status.ABORTED).build();
  }

  @Override
  public Dataset getDataset(UUID key) {
    return null;
  }

  @Override
  public List<UUID> getRunningValidations(int min) {
    return Collections.emptyList();
  }

  @Override
  public void delete(UUID key) {
    // NOTHING
  }
}
