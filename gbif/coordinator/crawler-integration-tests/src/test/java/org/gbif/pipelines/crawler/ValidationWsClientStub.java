package org.gbif.pipelines.crawler;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.dwc.terms.Term;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.ws.client.ValidationWsClient;

@Getter
@NoArgsConstructor(staticName = "create")
public class ValidationWsClientStub implements ValidationWsClient {

  private Validation validation = Validation.builder().key(UUID.randomUUID()).build();

  @Override
  public boolean reachedMaxRunningValidations(String userName) {
    return false;
  }

  @Override
  public Validation submitFile(File file) {
    return validation;
  }

  @Override
  public Validation validateFile(File file, ValidationRequest validationRequest) {
    return validation;
  }

  @Override
  public Validation validateFileFromUrl(String fileUrl, ValidationRequest validationRequest) {
    return validation;
  }

  @Override
  public PagingResponse<Validation> list(Map<String, Object> validationSearchRequest) {
    return new PagingResponse<>(0L, 1, 1L, Collections.singletonList(validation));
  }

  @Override
  public Validation get(UUID key) {
    validation.setKey(key);
    return validation;
  }

  @Override
  public Validation update(UUID key, Validation validation) {
    this.validation = validation;
    return validation;
  }

  @Override
  public Validation cancel(UUID key) {
    return validation;
  }

  @Override
  public void delete(UUID key) {
    // nothing
  }

  public Optional<FileInfo> getFileInfo(String term) {
    return validation.getMetrics().getFileInfos().stream()
        .filter(x -> term.equals(x.getRowType()))
        .findAny();
  }

  public Optional<FileInfo> getFileInfo(Term term) {
    return getFileInfo(term.qualifiedName());
  }

  public Optional<FileInfo> getFileInfoByFileType(DwcFileType fileType) {
    return validation.getMetrics().getFileInfos().stream()
        .filter(x -> x.getFileType() == fileType)
        .findAny();
  }
}
