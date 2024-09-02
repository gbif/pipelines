package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.Term;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class VocabularyConfig implements Serializable {

  private static final long serialVersionUID = -8686879236789318026L;

  // directory where the vocabulary files are stored
  private String vocabulariesPath;

  private Map<String, String> vocabulariesNames = Collections.emptyMap();

  public String getVocabularyFileName(Term term) {
    return vocabulariesNames.computeIfAbsent(
        term.qualifiedName(),
        t -> {
          throw new IllegalArgumentException("Can't find associated vocabulary for term " + t);
        });
  }
}
