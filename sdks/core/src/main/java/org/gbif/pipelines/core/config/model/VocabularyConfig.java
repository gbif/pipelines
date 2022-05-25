package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Optional;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

@Slf4j
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class VocabularyConfig implements Serializable {

  private static final long serialVersionUID = -8686879236789318025L;

  // directory where the vocabulary files are stored
  private String vocabulariesPath;

  // name of the lifeStage vocabulary
  private String lifeStageVocabName;

  // name of the establishmentMeans vocabulary
  private String establishmentMeansVocabName;

  // name of the pathway vocabulary
  private String pathwayVocabName;

  // name of the degreeOfEstablishment vocabulary
  private String degreeOfEstablishmentVocabName;

  public Optional<String> getVocabularyFileName(Term term) {
    if (term == DwcTerm.lifeStage) {
      return Optional.of(lifeStageVocabName);
    } else if (term == DwcTerm.establishmentMeans) {
      return Optional.of(establishmentMeansVocabName);
    } else if (term == DwcTerm.pathway) {
      return Optional.of(pathwayVocabName);
    } else if (term == DwcTerm.degreeOfEstablishment) {
      return Optional.of(degreeOfEstablishmentVocabName);
    }
    log.warn("Can't file mapping for the vocabulary term {}", term.qualifiedName());
    return Optional.empty();
  }
}
