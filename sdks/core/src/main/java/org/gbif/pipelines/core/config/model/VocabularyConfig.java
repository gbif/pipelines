package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class VocabularyConfig implements Serializable {

  private static final long serialVersionUID = -8686879236789318025L;

  // directory where the vocabulary files are stored
  private String vocabulariesPath;

  // name of the life stage vocabulary
  private String lifeStageVocabName;

  // name of the life stage vocabulary
  private String establishmentMeansVocabName;

  public String getVocabularyFileName(Term term) {
    if (term == DwcTerm.lifeStage) {
      return lifeStageVocabName;
    } else if (term == DwcTerm.establishmentMeans) {
      return establishmentMeansVocabName;
    }
    return null;
  }
}
