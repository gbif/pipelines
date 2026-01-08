package org.gbif.validator.persistence.mapper;

import jakarta.annotation.Nullable;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.ibatis.annotations.Param;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationSearchRequest;

/** MyBatis mapper for CRUD and list database operations. */
public interface ValidationMapper {

  /**
   * Gets a Validation by its key/identifier.
   *
   * @param key validation identifier
   */
  Validation get(@Param("key") UUID key);

  /**
   * Creates/persists a validation.
   *
   * @param validation to be created
   */
  void create(Validation validation);

  /**
   * Logical deletion of a validation.
   *
   * @param key validation identifier
   */
  void delete(@Param("key") UUID key);

  /**
   * Updates the allowed information of a validation, changes the modified date to now.
   *
   * @param validation to be updated
   */
  void update(Validation validation);

  /**
   * Paginates through validations, optionally filtered by username.
   *
   * @param username filter
   * @param searchRequest filter
   * @return a list of validation
   */
  List<Validation> list(
      @Nullable @Param("username") String username,
      @Nullable @Param("searchRequest") ValidationSearchRequest searchRequest);

  /**
   * Counts the number of validations of an optional user parameter.
   *
   * @param username filter
   * @param searchRequest filter
   * @return number of validations
   */
  int count(
      @Nullable @Param("username") String username,
      @Nullable @Param("searchRequest") ValidationSearchRequest searchRequest);

  /**
   * Returns validation UUID running or queued for more than specified time in min
   *
   * @param date more than specified date
   * @return list of UUID validations
   */
  List<UUID> getRunningValidations(@Param("date") Date date);
}
