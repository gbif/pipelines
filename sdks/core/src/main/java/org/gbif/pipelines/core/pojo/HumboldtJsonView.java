package org.gbif.pipelines.core.pojo;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.List;
import lombok.Data;
import org.gbif.pipelines.io.avro.Humboldt;

@Data
public class HumboldtJsonView {

  @JsonUnwrapped private Humboldt humboldt;

  private VocabularyList targetLifeStageScope;
  private VocabularyList excludedLifeStageScope;
  private VocabularyList targetDegreeOfEstablishmentScope;
  private VocabularyList excludedDegreeOfEstablishmentScope;

  @Data
  public static final class VocabularyList {
    private List<String> concepts;
    private List<String> lineage;
  }
}
