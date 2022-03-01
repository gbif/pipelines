package org.gbif.mail.validator;

import freemarker.template.TemplateException;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.GbifUser;
import org.gbif.mail.BaseEmailModel;
import org.gbif.mail.EmailSender;
import org.gbif.mail.EmailTemplateProcessor;
import org.gbif.mail.EmailType;
import org.gbif.mail.FreemarkerEmailTemplateProcessor;
import org.gbif.mail.UserHelperService;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.validator.api.Validation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** Freemarker based email service to send notifications about the validation status. */
@RequiredArgsConstructor
@Slf4j
@Service
public class ValidatorEmailServiceImpl implements ValidatorEmailService {

  private final EmailTemplateProcessor templateProcessor = new FreemarkerEmailTemplateProcessor();

  private final UserHelperService userHelperService;

  private final EmailSender emailSender;

  @Value("${gbif.portal.url}")
  private final String portalUrl;

  @Override
  public void sendEmailNotification(Validation validation) {
    buildEmailModel(validation).ifPresent(emailSender::send);
  }

  /**
   * Builds the email template if notification emails address is found from the validation or user.
   */
  private Optional<BaseEmailModel> buildEmailModel(Validation validation) {
    return typeFromStatus(validation)
        .flatMap(
            t ->
                userHelperService
                    .getUser(validation.getUsername())
                    .map(user -> getBaseEmailModel(validation, t, user)));
  }

  /** Hides checked exceptions. */
  private BaseEmailModel getBaseEmailModel(Validation validation, EmailType t, GbifUser user) {
    try {
      return templateProcessor.buildEmail(
          t,
          getNotificationAddresses(validation, user),
          ValidatorTemplateDataModel.modelBuilder()
              .validation(validation)
              .portalUrl(portalUrl)
              .build(),
          getLocale(user));
    } catch (IOException | TemplateException ex) {
      throw new PipelinesException(ex);
    }
  }

  /** Gets the locale from the user or through the validation.installationKey */
  private Locale getLocale(GbifUser user) {
    if (user != null) {
      return userHelperService.getUserLocaleOrDefault(user);
    }
    return Locale.ENGLISH;
  }

  /** Gets a notification address form the validation or user. */
  private Set<String> getNotificationAddresses(Validation validation, GbifUser user) {
    if (validation.getNotificationEmails() != null
        && !validation.getNotificationEmails().isEmpty()) {
      return validation.getNotificationEmails();
    }
    return getUserNotificationAddresses(user);
  }

  /** Gets the user's email address */
  private Set<String> getUserNotificationAddresses(GbifUser user) {
    return Optional.ofNullable(user)
        .filter(u -> u.getEmail() != null)
        .map(u -> Collections.singleton(u.getEmail()))
        .orElse(Collections.emptySet());
  }

  /** Gets the email type from validation status. */
  private Optional<EmailType> typeFromStatus(Validation validation) {
    if (validation.succeeded()) {
      return Optional.of(ValidatorEmailType.SUCCESSFUL);
    }
    if (validation.failed()) {
      return Optional.of(ValidatorEmailType.FAILED);
    }
    return Optional.empty();
  }
}
