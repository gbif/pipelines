package org.gbif.mail.validator;

import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import org.gbif.mail.BaseEmailModel;
import org.gbif.mail.EmailType;

@Getter
public class ValidatorEmailModel extends BaseEmailModel {

  private final EmailType emailType;

  @Builder(builderMethodName = "modelBuilder")
  public ValidatorEmailModel(
      Set<String> emailAddresses,
      String subject,
      String body,
      Set<String> ccAddresses,
      EmailType emailType) {
    super(emailAddresses, subject, body, ccAddresses);
    this.emailType = emailType;
  }
}
