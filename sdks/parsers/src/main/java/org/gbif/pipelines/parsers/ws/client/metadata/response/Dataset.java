package org.gbif.pipelines.parsers.ws.client.metadata.response;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Can be a org.gbif.api.model.registry.Dataset model, some problem with enum unmarshalling */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dataset implements Serializable {

  private static final long serialVersionUID = 4190160247363021997L;

  private String installationKey;
  private String publishingOrganizationKey;
  private String license;

  public String getInstallationKey() {
    return installationKey;
  }

  public void setInstallationKey(String installationKey) {
    this.installationKey = installationKey;
  }

  public String getPublishingOrganizationKey() {
    return publishingOrganizationKey;
  }

  public void setPublishingOrganizationKey(String publishingOrganizationKey) {
    this.publishingOrganizationKey = publishingOrganizationKey;
  }

    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }
}
