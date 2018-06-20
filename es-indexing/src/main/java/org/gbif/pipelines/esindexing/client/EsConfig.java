package org.gbif.pipelines.esindexing.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** ES configuration. */
public class EsConfig {

  private final List<URL> hosts;

  private EsConfig(String[] hostsAddresses) {
    Objects.requireNonNull(hostsAddresses);

    hosts = new ArrayList<>();
    Arrays.stream(hostsAddresses)
        .forEach(
            address -> {
              try {
                hosts.add(new URL(address));
              } catch (MalformedURLException e) {
                throw new IllegalArgumentException(address + " is not a valid url", e);
              }
            });
  }

  /**
   * Creates a {@link EsConfig} from the addresses received.
   *
   * @param hostsAddresses they should be valid URLs.
   * @return {@link EsConfig}.
   */
  public static EsConfig from(String... hostsAddresses) {
    return new EsConfig(hostsAddresses);
  }

  public List<URL> getHosts() {
    return hosts;
  }
}
