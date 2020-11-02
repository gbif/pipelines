package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Properties;
import javax.validation.constraints.NotNull;
import lombok.ToString;
import org.gbif.cli.ConfigUtils;
import org.gbif.cli.PropertyName;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;

/**
 * A configuration class which can be used to get all the details needed to create a writable
 * connection to the GBIF registry.
 */
@ToString
public class RegistryConfiguration {

  @Parameter(names = "--registry-ws")
  @PropertyName("registry.ws.url")
  @NotNull
  public String wsUrl = "http://api.gbif.org/v1";

  @Parameter(names = "--registry-user")
  @NotNull
  public String user;

  @Parameter(names = "--registry-password", password = true)
  @NotNull
  public String password;

  /**
   * Convenience method to setup a guice injector with a writable registry client module using the
   * configuration of this instance.
   *
   * @return guice injector with RegistryWsClientModule bound
   */
  public Injector newRegistryInjector() {
    // setup writable registry client
    Properties properties = ConfigUtils.toProperties(this);
    RegistryWsClientModule regModule = new RegistryWsClientModule(properties);
    SingleUserAuthModule authModule = new SingleUserAuthModule(user, password);

    return Guice.createInjector(regModule, authModule);
  }
}
