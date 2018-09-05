package org.gbif.pipelines.minipipelines;

import org.gbif.pipelines.minipipelines.DwcaPipelineOptions.GbifEnv;
import org.gbif.pipelines.parsers.ws.config.WsConfig;
import org.gbif.pipelines.parsers.ws.config.WsConfigFactory;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import static org.gbif.pipelines.minipipelines.DwcaPipelineOptions.GbifEnv.DEV;
import static org.gbif.pipelines.minipipelines.DwcaPipelineOptions.GbifEnv.PROD;
import static org.gbif.pipelines.minipipelines.DwcaPipelineOptions.GbifEnv.UAT;

class HttpConfigFactory {

  private static final Map<GbifEnv, String> ENV_MAP = new EnumMap<>(GbifEnv.class);

  static {
    ENV_MAP.put(DEV, "https://api.gbif-dev.org/");
    ENV_MAP.put(UAT, "https://api.gbif-uat.org/");
    ENV_MAP.put(PROD, "https://api.gbif.org/");
  }

  private HttpConfigFactory() {}

  static WsConfig getConfig(GbifEnv env) {
    return Optional.ofNullable(ENV_MAP.get(env))
        .map(WsConfigFactory::createFromUrl)
        .orElseThrow(() -> new IllegalArgumentException(env + " environment not supported"));
  }

  static WsConfig getConfig(GbifEnv env, String api) {
    return Optional.ofNullable(ENV_MAP.get(env))
        .map(x -> WsConfigFactory.createFromUrl(x + api))
        .orElseThrow(() -> new IllegalArgumentException(env + " environment not supported"));
  }
}
