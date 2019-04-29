package org.gbif.pipelines.parsers.config;

import org.aeonbits.owner.Config;

/** Models the ws configuration. If you want to create an istance, use {@link WsConfigFactory} */
public interface WsConfig extends Config {

  @Key("gbif.api.url")
  String getBasePath();

  @Key("metadata.ws.timeout")
  @DefaultValue("60")
  long getMetadataTimeout();

  @Key("blast.ws.url")
  String getBlastUrl();

  @Key("blast.ws.timeout")
  @DefaultValue("60")
  long getBlastTimeout();
}
