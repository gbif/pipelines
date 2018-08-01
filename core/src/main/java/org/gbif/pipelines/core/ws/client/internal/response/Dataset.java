package org.gbif.pipelines.core.ws.client.internal.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Dataset {

  private String installationKey;
  private String publishingOrganizationKey;

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
}
