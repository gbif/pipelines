package org.gbif.pipelines.backbone.impact.v2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.ToString;
import org.gbif.nameparser.api.Rank;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
public class NameUsageMatchV2 implements Serializable {

  private boolean synonym;
  private RankedName usage;
  private RankedName acceptedUsage;
  private List<RankedName> classification = new ArrayList<>();
  private List<NameUsageMatchV2> alternatives = new ArrayList<>();
  private NameUsageMatchV2.Diagnostics diagnostics = new NameUsageMatchV2.Diagnostics();
  private List<Status> additionalStatus = new ArrayList<>();

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Diagnostics {
    private MatchType matchType;
    private List<String> issues;
    private Integer confidence;
    private String status;
    private String note;
    private List<NameUsageMatchV2> alternatives;
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Status {
    private String datasetKey;
    private String gbifKey;
    private String datasetTitle;
    private String category;
  }

  @Data
  public static class RankedName {
    private String key;
    private String name;
    private String code;
    private Rank rank;
  }

  public enum MatchType {
    EXACT,
    VARIANT,
    CANONICAL,
    AMBIGUOUS,
    NONE,
    UNSUPPORTED,
    HIGHERRANK;
  }
}
