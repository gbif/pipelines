package org.gbif.mail.validator;

import lombok.Builder;
import lombok.Getter;
import org.gbif.mail.BaseTemplateDataModel;
import org.gbif.validator.api.Validation;

@Getter
public class ValidatorTemplateDataModel extends BaseTemplateDataModel {

  private final Validation validation;

  private final String portalUrl;

  private final String validatorUrl;

  @Builder(builderMethodName = "modelBuilder")
  public ValidatorTemplateDataModel(
      String name, Validation validation, String portalUrl, String validatorUrl) {
    super(name);
    this.validation = validation;
    this.portalUrl = portalUrl;
    this.validatorUrl = validatorUrl;
  }
}
