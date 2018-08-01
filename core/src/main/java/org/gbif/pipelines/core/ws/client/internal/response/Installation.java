package org.gbif.pipelines.core.ws.client.internal.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Installation {

  private String organizationKey;

  public String getOrganizationKey() {
    return organizationKey;
  }

  public void setOrganizationKey(String organizationKey) {
    this.organizationKey = organizationKey;
  }
}
