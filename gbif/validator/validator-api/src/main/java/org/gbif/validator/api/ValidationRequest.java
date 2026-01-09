package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import jakarta.validation.constraints.Email;
import java.util.Set;
import java.util.UUID;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = ValidationRequest.ValidationRequestBuilder.class)
public class ValidationRequest {

  /** Identifier at the source. */
  private String sourceId;

  /** GBIF Installation from where the validation started. */
  private UUID installationKey;

  /** Notification email addresses. */
  private Set<@Email String> notificationEmail;
}
