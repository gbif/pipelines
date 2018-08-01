package org.gbif.pipelines.core.ws.client.internal.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Organization {

  private String endorsingNodeKey;

  public String getEndorsingNodeKey() {
    return endorsingNodeKey;
  }

  public void setEndorsingNodeKey(String endorsingNodeKey) {
    this.endorsingNodeKey = endorsingNodeKey;
  }
}
