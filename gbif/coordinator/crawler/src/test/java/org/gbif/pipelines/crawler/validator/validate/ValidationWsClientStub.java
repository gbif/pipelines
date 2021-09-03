package org.gbif.pipelines.crawler.validator.validate;

import java.io.File;
import java.util.Set;
import java.util.UUID;
import javax.validation.constraints.Email;
import lombok.Builder;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

@Builder
public class ValidationWsClientStub implements ValidationWsClient {

  private final UUID key;
  @Builder.Default private Validation validation = Validation.builder().build();
  private Status status;

  @Override
  public Validation submitFile(File file) {
    return null;
  }

  @Override
  public Validation submitFile(
      File file, String sourceId, UUID installationKey, Set<@Email String> notificationEmails) {
    return null;
  }

  @Override
  public Validation submitUrl(
      String fileUrl,
      String sourceId,
      UUID installationKey,
      Set<@Email String> notificationEmails) {
    return null;
  }

  @Override
  public Validation submitUrl(String fileUrl) {
    return null;
  }

  @Override
  public PagingResponse<Validation> list(Pageable page, Set<Status> statuses) {
    return null;
  }

  @Override
  public Validation get(UUID key) {
    return validation;
  }

  @Override
  public void update(UUID key, Validation validation) {
    this.validation = validation;
  }

  @Override
  public void update(Validation validation) {
    this.validation = validation;
  }

  @Override
  public void cancel(UUID key) {
    this.status = Status.ABORTED;
  }
}
