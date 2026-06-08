package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DerivedMetadataConfig implements Serializable {

  /** Maximum number of taxons to associate with an event in derived metadata */
  int maxTaxonCoveragePerEvent = 2000;
}
