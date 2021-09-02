package org.gbif.mail.validator;

import org.gbif.validator.api.Validation;

/** Service to send notification emails for data validations. */
public interface ValidatorEmailService {

  /** Sends an email for a finished validation. */
  void sendEmailNotification(Validation validation);
}
