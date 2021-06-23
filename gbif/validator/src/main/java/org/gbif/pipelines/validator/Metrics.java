package org.gbif.pipelines.validator;

import java.util.Map;
import lombok.Builder;
import lombok.Data;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;

@Builder
@Data
public class Metrics {

  private Map<Term, Long> coreTermsCountMap;
  private Map<Extension, Map<Term, Long>> extensionsTermsCountMap;
}
