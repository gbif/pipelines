package org.gbif.pipelines.core.pojo;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.gbif.pipelines.io.avro.Humboldt;

@Data
public class HumboldtJsonView {

  @JsonUnwrapped private Humboldt humboldt;

  private VocabularyList targetLifeStageScope;
  private VocabularyList excludedLifeStageScope;
  private VocabularyList targetDegreeOfEstablishmentScope;
  private VocabularyList excludedDegreeOfEstablishmentScope;
  private Map<String, List<Classification>> targetTaxonomicScope = new HashMap<>();
  private Map<String, Map<String, Set<String>>> humboldtTargetTaxonClassifications =
      new HashMap<>();
  private Map<String, List<Classification>> excludedTaxonomicScope = new HashMap<>();
  private Map<String, List<Classification>> absentTaxa = new HashMap<>();
  private Map<String, List<Classification>> nonTargetTaxa = new HashMap<>();

  @Data
  public static final class VocabularyList {
    private List<String> concepts;
    private List<String> lineage;
  }

  @Data
  public static final class Classification {
    private String usageKey;
    private String usageName;
    private String usageRank;
    private List<org.gbif.pipelines.io.avro.RankedName> rankedNames;
  }
}
