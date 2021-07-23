package org.gbif.mail.validator;

import org.gbif.validator.api.Validation;

public interface ValidatorEmailService {

  void sendEmailNotification(Validation validation);
}
