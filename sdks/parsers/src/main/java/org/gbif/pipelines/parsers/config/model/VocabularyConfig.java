package org.gbif.pipelines.parsers.config.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class VocabularyConfig implements Serializable {

  private static final long serialVersionUID = -8686879236789318025L;

  // directory where the vocabulary files are stored
  private String vocabulariesPath;

  // name of the life stage vocabulary
  private String lifeStageVocabName;

}
