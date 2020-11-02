package org.gbif.pipelines.core.ws.metadata.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.gbif.api.model.registry.MachineTag;

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
  private String collectionKey;
  private String institutionKey;
  private String license;
  private String title;
  private Project project;
  private List<MachineTag> machineTags;
}
