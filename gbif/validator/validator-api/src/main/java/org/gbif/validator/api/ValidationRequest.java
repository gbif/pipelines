package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Size;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ValidationRequest {

  /** Identifier at the source. */
  private String sourceId;

  /** GBIF Installation from where the validation started. */
  private UUID installationKey;

  public static final int MAX_NOTIFICATION_EMAILS = 10;

  /** Notification email addresses. */
  @Size(max = MAX_NOTIFICATION_EMAILS)
  private Set<@Email String> notificationEmail;
}
