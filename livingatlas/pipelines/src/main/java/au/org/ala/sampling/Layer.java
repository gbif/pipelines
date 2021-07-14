package au.org.ala.sampling;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Getter;
import lombok.Setter;

/** A spatial layer in stored and served by the spatial service. */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class Layer {

  private String id;
  private Boolean enabled;
  private Long dt_added;
  private String source_link;
  private String datalang;
  private String environmentalvaluemin;
  private String shape;
  private String classification2;
  private String displayname;
  private Boolean grid;
  private String path_orig;
  private Double minlongitude;
  private Double maxlatitude;
  private String mddatest;
  private Double maxlongitude;
  private Double minlatitude;
  private String scale;
  private String displaypath;
  private String path;
  private String name;
  private String metadatapath;
  private String type;
  private String notes;
  private String uid;
  private String licence_notes;
  private String environmentalvaluemax;
  private String classification1;
  private String environmentalvalueunits;
  private String domain;
  private String description;
  private String licence_level;
  private String source;
  private String citation_date;
}
