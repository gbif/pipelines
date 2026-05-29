package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessagingConfig implements Serializable {
  String host;
  Integer port;
  String virtualHost;
  String username;
  String password;
  Integer prefetchCount;
}
