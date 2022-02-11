package org.gbif.validator.service;

import java.util.List;
import java.util.UUID;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;

/**
 * Data validation service.
 *
 * @param <MF> multi-part file upload type
 */
public interface ValidationService<MF> {

  boolean reachedMaxRunningValidations(String userName);

  Validation validateFile(MF file, ValidationRequest validationRequest);

  Validation validateFileFromUrl(String fileURL, ValidationRequest validationRequest);

  Validation get(UUID key);

  Validation update(Validation validation);

  Validation cancel(UUID key);

  void delete(UUID key);

  PagingResponse<Validation> list(ValidationSearchRequest searchRequest);

  Dataset getDataset(UUID key);

  List<UUID> getRunningValidations(int min);
}
