package org.gbif.pipelines.core.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SerDeUtils {

  public interface DnaDerivedDataMixin {

    @JsonProperty("nFraction")
    Double getNFraction();

    @JsonProperty("nFraction")
    void setNFraction(Double nFraction);

    @JsonProperty("nRunsCapped")
    Integer getNRunsCapped();

    @JsonProperty("nRunsCapped")
    void setNRunsCapped(Integer nRunsCapped);
  }
}
