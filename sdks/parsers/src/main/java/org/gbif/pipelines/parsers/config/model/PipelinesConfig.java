package org.gbif.pipelines.parsers.config.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PipelinesConfig implements Serializable {

  private static final long serialVersionUID = 8102560635064341713L;

  private String zkConnectionString;

  private WsConfig gbifApi;

  private String imageCachePath = "bitmap/bitmap.png";

  private KvConfig nameUsageMatch;

  private KvConfig geocode;

  private KvConfig locationFeature;

  private ContentConfig content;

  private WsConfig amplification;

  private KeygenConfig keygen;

  private LockConfig indexLock;

  private LockConfig hdfsLock;
}
