package org.gbif.pipelines.backbone.impact.v2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.ToString;
import org.gbif.nameparser.api.Authorship;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@ToString
public class NameUsageMatchV2 implements Serializable {

  private boolean synonym;
  private Usage usage;
  private Usage acceptedUsage;
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
  public static class Authorship {
    private List<String> authors = new ArrayList();
    private List<String> exAuthors = new ArrayList();
    private String year;
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Status {
    private String datasetKey;
    private String gbifKey;
    private String datasetAlias;
    private String status;
    private String statusCode;
    private String sourceId;
  }

  @Data
  public static class Usage {
    private String key;
    private String name;
    private String canonicalName;
    private String authorship;
    private String rank;
    private String code;
    private String uninomial;
    private String genus;
    private String infragenericEpithet;
    private String specificEpithet;
    private String infraspecificEpithet;
    private String cultivarEpithet;
    private String phrase;
    private String voucher;
    private String nominatingParty;
    private boolean candidatus;
    private String notho;
    private Boolean originalSpelling;
    private Map<String, String> epithetQualifier;
    private String type;
    protected boolean extinct;
    private Authorship combinationAuthorship;
    private Authorship basionymAuthorship;
    private String sanctioningAuthor;
    private String taxonomicNote;
    private String nomenclaturalNote;
    private String publishedIn;
    private String unparsed;
    private boolean doubtful;
    private boolean manuscript;
    private String state;
    private Set<String> warnings;
    // additional flags
    private boolean isAbbreviated;
    private boolean isAutonym;
    private boolean isBinomial;
    private boolean isTrinomial;
    private boolean isIncomplete;
    private boolean isIndetermined;
    private boolean isPhraseName;
    private String terminalEpithet;
  }

  @Data
  public static class RankedName {
    private String key;
    private String name;
    private String rank;
    private String code;
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
