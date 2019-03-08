package org.gbif.pipelines.parsers.ws.client.metadata.response;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Can be a org.gbif.api.model.registry.Dataset model, some problem with enum unmarshalling */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dataset implements Serializable {

  private static final long serialVersionUID = 4190160247363021998L;

  private String installationKey;
  private String publishingOrganizationKey;
  private String license;
  private String title;
}
