package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.sql.Date;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
// Constructors are needed for MyBatis, persistence layer.
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = Validation.ValidationBuilder.class)
public class Validation {

  private static final EnumSet<Status> EXECUTING_STATUSES =
      EnumSet.of(Status.SUBMITTED, Status.DOWNLOADING, Status.RUNNING);

  private static final EnumSet<Status> FINISHED_STATUSES =
      EnumSet.of(Status.FINISHED, Status.FAILED, Status.ABORTED);

  public enum Status {
    DOWNLOADING,
    SUBMITTED,
    RUNNING,
    FINISHED,
    ABORTED,
    FAILED
  }

  /** Validation key/identifier. */
  private UUID key;

  /** Identifier at the source. */
  private String sourceId;

  /** GBIF Installation from where the validation started. */
  private UUID installationKey;

  /** Notification email addresses. */
  @JsonIgnore private Set<String> notificationEmails;

  /** Timestamp when the validation was created. */
  private Date created;

  /** Last modified timestamp. */
  private Date modified;

  /** User that triggered or owns the validation. */
  private String username;

  /** File name to be validated. */
  private String file;

  /** File size in bytes. */
  private Long fileSize;

  /** Detected file format. */
  private FileFormat fileFormat;

  /** Validation status. */
  private Status status;

  /** Validation result. */
  private Metrics metrics;

  public static Set<Status> executingStatuses() {
    return EXECUTING_STATUSES;
  }

  public static Set<Status> finishedStatuses() {
    return FINISHED_STATUSES;
  }

  @JsonIgnore
  public boolean succeeded() {
    return Status.FINISHED == this.status;
  }

  @JsonIgnore
  public boolean failed() {
    return Status.ABORTED == this.status || Status.FAILED == this.status;
  }

  @JsonIgnore
  public boolean isExecuting() {
    return EXECUTING_STATUSES.contains(status);
  }

  @JsonIgnore
  public boolean hasFinished() {
    return FINISHED_STATUSES.contains(status);
  }

  public enum ErrorCode {
    MAX_RUNNING_VALIDATIONS,
    MAX_FILE_SIZE_VIOLATION,
    AUTHORIZATION_ERROR,
    NOT_FOUND,
    IO_ERROR,
    VALIDATION_IS_NOT_EXECUTING,
    NOTIFICATION_EMAILS_MISSING,
    WRONG_KEY_IN_REQUEST
  }
}
