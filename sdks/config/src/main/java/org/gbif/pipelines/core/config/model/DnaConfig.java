package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DnaConfig implements Serializable {

  int batchSizeSequenceClassifier = 500;
  WsConfig sequenceClassifierWsConfig;
  boolean enableDnaTaxonInterpretation = false;
  // checklists to match the DNA-assisted classifications against, the default taxonomy when empty
  List<String> checklistKeys = new ArrayList<>();
  float nucleotideSequenceHighNFractionThreshold = 0.05f;
  float nucleotideSequenceHighAmbiguityThreshold = 0.05f;
}
