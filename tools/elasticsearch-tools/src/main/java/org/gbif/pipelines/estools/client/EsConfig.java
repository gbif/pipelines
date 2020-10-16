package org.gbif.pipelines.estools.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;

/** ES configuration. */
public class EsConfig {

  private final List<URL> hosts;
  private final String[] rawHosts;

  private EsConfig(@NonNull String[] hostsAddresses) {

    this.rawHosts = hostsAddresses;
    this.hosts =
        Arrays.stream(hostsAddresses)
            .map(
                address -> {
                  try {
                    return new URL(address);
                  } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(address + " is not a valid url", e);
                  }
                })
            .collect(Collectors.toList());
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

  public String[] getRawHosts() {
    return rawHosts;
  }
}
