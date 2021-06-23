package org.gbif.pipelines.validator;

import java.util.Map;

import org.gbif.dwc.terms.Term;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Metrics {

  private Map<Term, Long> coreTermsCountMap;
  private Map<Term, Map<Term, Long>> extensionsTermsCountMap;
}
