package org.gbif.pipelines.parsers.ws.client.metadata.response;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Can be a org.gbif.api.model.registry.Installation model, some problem with enum unmarshalling */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Installation implements Serializable {

  private static final long serialVersionUID = -4294360703275377726L;

  private String organizationKey;

  private String protocol;

  public String getOrganizationKey() {
    return organizationKey;
  }

  public void setOrganizationKey(String organizationKey) {
    this.organizationKey = organizationKey;
  }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }
}
