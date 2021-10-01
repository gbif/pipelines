package org.gbif.pipelines.validator.checklists;

import lombok.Builder;
import lombok.Data;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.checklistbank.VerbatimNameUsage;
import org.gbif.checklistbank.model.UsageExtensions;

@Data
@Builder
/** Object holder for the results of Checklists normalization. */
public class NormalizedNameUsageData {

  private final VerbatimNameUsage verbatimNameUsage;

  private final NameUsage nameUsage;

  private final ParsedName parsedName;

  private final UsageExtensions usageExtensions;
}
