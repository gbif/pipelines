package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.gbif.dwc.terms.Term;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClbDatasetImport {

  private int datasetKey;
  private int attempt;
  private State state;
  private Long bareNameCount;
  private Long distributionCount;
  private Long estimateCount;
  private Long mediaCount;
  private Long nameCount;
  private Long referenceCount;
  private Long synonymCount;
  private Long taxonCount;
  private Long treatmentCount;
  private Long typeMaterialCount;
  private Long vernacularCount;
  private Long usagesCount;
  private Map<String, Long> issuesCount = new HashMap<>();
  private Map<Term, Long> verbatimByTermCount = new HashMap<>();
  private Map<Term, Map<Term, Long>> verbatimByRowTypeCount = new HashMap<>();

  public enum State {
    finished,
    canceled,
    failed
  }
}
