package org.gbif.pipelines.parsers.ws.client.metadata.response;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Can be a org.gbif.api.model.registry.Organization model, some problem with enum unmarshalling */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Organization implements Serializable {

  private static final long serialVersionUID = -2749510036624325642L;

  private String endorsingNodeKey;

  private String country;

  public String getEndorsingNodeKey() {
    return endorsingNodeKey;
  }

  public void setEndorsingNodeKey(String endorsingNodeKey) {
    this.endorsingNodeKey = endorsingNodeKey;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }
}
