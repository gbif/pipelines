package org.gbif.pipelines.estools.model;

import static org.gbif.pipelines.estools.service.EsConstants.Util.INDEX_SEPARATOR;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.nio.file.Path;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.gbif.pipelines.estools.common.SettingsType;

/** Index parameters holder. */
@Builder
@Getter
@ToString
public class IndexParams {

  private String indexName;
  private String datasetKey;
  private Integer attempt;
  private SettingsType settingsType;
  private Map<String, String> settings;
  private Path pathMappings;
  private String mappings;

  public String getIndexName() {
    return Strings.isNullOrEmpty(this.indexName)
        ? createIndexName(this.datasetKey, this.attempt)
        : this.indexName;
  }

  private static String createIndexName(String datasetId, int attempt) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetId), "dataset id is required");
    return datasetId + INDEX_SEPARATOR + attempt;
  }
}
