package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class DnaConfig implements Serializable {

  float nucleotideSequenceHighNFractionThreshold = 0.05f;
  float nucleotideSequenceHighAmbiguityThreshold = 0.05f;

}
