package org.gbif.pipelines.core.pojo;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;
import org.gbif.pipelines.io.avro.Humboldt;

@Data
public class HumboldtJsonView {

  @JsonUnwrapped private Humboldt humboldt;

  private VocabularyList targetLifeStageScope;
  private VocabularyList excludedLifeStageScope;
  private VocabularyList targetDegreeOfEstablishmentScope;
  private VocabularyList excludedDegreeOfEstablishmentScope;
  private Map<String, Map<String, List<String>>> targetTaxonomicScope = new HashMap<>();
  private Map<String, Map<String, List<String>>> excludedTaxonomicScope = new HashMap<>();
  private Map<String, Map<String, List<String>>> absentTaxa = new HashMap<>();
  private Map<String, Map<String, List<String>>> nonTargetTaxa = new HashMap<>();

  @Data
  public static final class VocabularyList {
    private List<String> concepts;
    private List<String> lineage;
  }
}
