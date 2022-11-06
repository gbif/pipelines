package org.gbif.pipelines.core.ws.metadata.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.gbif.api.model.registry.Endpoint;

/** Can be a org.gbif.api.model.registry.Installation model, some problem with enum unmarshalling */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Installation implements Serializable {

  private static final long serialVersionUID = -2149391644429238002L;

  private String key;
  private String organizationKey;

  private List<Endpoint> endpoints;
}
