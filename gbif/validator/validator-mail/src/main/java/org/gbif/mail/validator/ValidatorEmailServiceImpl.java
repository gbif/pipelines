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
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.service.registry.InstallationService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.mail.BaseEmailModel;
import org.gbif.mail.EmailSender;
import org.gbif.mail.EmailTemplateProcessor;
import org.gbif.mail.EmailType;
import org.gbif.mail.FreemarkerEmailTemplateProcessor;
import org.gbif.mail.UserHelperService;
import org.gbif.validator.api.Validation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Slf4j
@Service
/** Freemarker based email service to send notifications about the validation status. */
public class ValidatorEmailServiceImpl implements ValidatorEmailService {

  private final EmailTemplateProcessor templateProcessor = new FreemarkerEmailTemplateProcessor();

  private final UserHelperService userHelperService;

  private final EmailSender emailSender;

  private final InstallationService installationService;

  private final OrganizationService organizationService;

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
          getLocale(user, validation));
    } catch (IOException | TemplateException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Gets the locale from the user or through the validation.installationKey */
  private Locale getLocale(GbifUser user, Validation validation) {
    if (user != null) {
      return userHelperService.getUserLocaleOrDefault(user);
    }
    if (validation.getInstallationKey() != null) {
      Installation installation = installationService.get(validation.getInstallationKey());
      if (installation != null && installation.getOrganizationKey() != null) {
        Organization organization = organizationService.get(installation.getOrganizationKey());
        if (organization != null && organization.getLanguage() != null) {
          return organization.getLanguage().getLocale();
        }
      }
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
