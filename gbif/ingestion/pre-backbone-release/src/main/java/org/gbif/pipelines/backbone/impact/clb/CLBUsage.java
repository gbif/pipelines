package org.gbif.pipelines.backbone.impact.clb;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CLBUsage implements Serializable {
  String id;
  String name;
  String label;
  String authorship;
  String rank;
  String code;
  String status;
  String parent;
  Integer canonicalId;
  Integer namesIndexId;
  String namesIndexMatchType;
  Integer sectorKey;
  String publishedInID;
}
