package org.gbif.mail.validator;

import io.vavr.control.Try;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.GbifUser;
import org.gbif.mail.BaseEmailModel;
import org.gbif.mail.EmailTemplateProcessor;
import org.gbif.mail.EmailType;
import org.gbif.mail.UserHelperService;
import org.gbif.validator.api.Validation;

@RequiredArgsConstructor
@Slf4j
public class ValidatorEmailFactory {

  private EmailTemplateProcessor templateProcessor;

  private UserHelperService userHelperService;

  public Optional<BaseEmailModel> buildEmailModel(Validation validation, String portalUrl) {
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
                                            ValidatorTemplateDataModel.builder()
                                                // .validation(validation)
                                                // .portalUrl(portalUrl)
                                                .build(),
                                            userHelperService.getLocale(user)))
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
