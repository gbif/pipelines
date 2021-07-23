package org.gbif.mail.validator;

import io.vavr.control.Try;
import java.util.Collections;
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
import org.gbif.validator.api.Validation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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

  private Optional<BaseEmailModel> buildEmailModel(Validation validation) {
    return typeFromStatus(validation)
        .flatMap(
            t ->
                userHelperService
                    .getUser(validation.getUsername())
                    .map(
                        user ->
                            Try.of(
                                    () ->
                                        templateProcessor.buildEmail(
                                            t,
                                            getNotificationAddresses(user),
                                            ValidatorTemplateDataModel.modelBuilder()
                                                .validation(validation)
                                                .portalUrl(portalUrl)
                                                .build(),
                                            userHelperService.getUserLocaleOrDefault(user)))
                                .get()));
  }

  private Set<String> getNotificationAddresses(GbifUser user) {
    return Optional.ofNullable(user)
        .filter(u -> u.getEmail() != null)
        .map(u -> Collections.singleton(u.getEmail()))
        .orElse(Collections.emptySet());
  }

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
