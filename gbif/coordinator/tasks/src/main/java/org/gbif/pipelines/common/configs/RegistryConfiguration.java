package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import lombok.ToString;
import org.gbif.cli.PropertyName;

/**
 * A configuration class which can be used to get all the details needed to create a writable
 * connection to the GBIF registry.
 */
@ToString
public class RegistryConfiguration {

  @Parameter(names = "--registry-ws")
  @PropertyName("registry.ws.url")
  public String wsUrl = "http://api.gbif.org/v1";

  @Parameter(names = "--registry-user")
  public String user;

  @Parameter(names = "--registry-password", password = true)
  public String password;
}
