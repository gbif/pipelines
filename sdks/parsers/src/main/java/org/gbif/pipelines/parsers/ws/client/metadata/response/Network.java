package org.gbif.pipelines.parsers.ws.client.metadata.response;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Can be a org.gbif.api.model.registry.Network model, some problem with enum unmarshalling */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Network implements Serializable {

  private static final long serialVersionUID = -8618844988822301908L;
}
