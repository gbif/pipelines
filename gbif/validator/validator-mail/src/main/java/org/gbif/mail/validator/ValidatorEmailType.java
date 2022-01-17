package org.gbif.mail.validator;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.gbif.mail.EmailType;
import org.springframework.context.support.ResourceBundleMessageSource;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum ValidatorEmailType implements EmailType {
  SUCCESSFUL("successfulValidation", "successful_validation.ftl"),

  FAILED("failedValidation", "failed_validation.ftl");

  private static final ResourceBundleMessageSource MESSAGE_SOURCE;

  static {
    MESSAGE_SOURCE = new ResourceBundleMessageSource();
    MESSAGE_SOURCE.setBasename("email/subjects/validation_email_subjects");
    MESSAGE_SOURCE.setDefaultEncoding(StandardCharsets.UTF_8.displayName());
  }

  private final String key;
  private final String template;

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getTemplate() {
    return template;
  }

  @Override
  public String getSubject(Locale locale, String... subjectParams) {
    return MESSAGE_SOURCE.getMessage(this.getKey(), subjectParams, locale);
  }
}
