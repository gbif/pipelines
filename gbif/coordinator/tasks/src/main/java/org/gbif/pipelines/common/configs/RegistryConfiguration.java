package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import lombok.ToString;
import org.gbif.cli.PropertyName;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.time.Duration;

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

  /**
   * Convenience method to provide a ws client factory. The factory will be used to create writable
   * registry clients.
   *
   * @return writable client factory
   */
  public ClientBuilder newClientBuilder() {
    // setup writable registry client
    return new ClientBuilder()
            .withUrl(wsUrl)
            .withCredentials(user, password)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            // This will give up to 40 tries, from 2 to 75 seconds apart, over at most 13 minutes (approx)
            .withExponentialBackoffRetry(Duration.ofSeconds(2), 1.1, 40);
  }
}
