package org.gbif.pipelines.core.ws.metadata.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Can´t be an org.gbif.api.model.registry.Organization due to enum unmarshalling */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Organization implements Serializable {

  private static final long serialVersionUID = -2749510036624325642L;

  private String endorsingNodeKey;
  private String country;
  private String title;
}
