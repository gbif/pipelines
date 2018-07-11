package org.gbif.pipelines.minipipelines.dwca;

import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.HttpConfigFactory;
import org.gbif.pipelines.minipipelines.dwca.DwcaPipelineOptions.GbifEnv;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import static org.gbif.pipelines.minipipelines.dwca.DwcaPipelineOptions.GbifEnv.DEV;
import static org.gbif.pipelines.minipipelines.dwca.DwcaPipelineOptions.GbifEnv.PROD;
import static org.gbif.pipelines.minipipelines.dwca.DwcaPipelineOptions.GbifEnv.UAT;

class WsConfigFactory {

  private static final Map<GbifEnv, String> ENV_MAP = new EnumMap<>(GbifEnv.class);

  static {
    ENV_MAP.put(DEV, "https://api.gbif-dev.org/");
    ENV_MAP.put(UAT, "https://api.gbif-uat.org/");
    ENV_MAP.put(PROD, "https://api.gbif.org/");
  }

  private WsConfigFactory() {}

  static Config getConfig(GbifEnv env) {
    return Optional.ofNullable(ENV_MAP.get(env))
        .map(HttpConfigFactory::createConfigFromUrl)
        .orElseThrow(() -> new IllegalArgumentException(env + " environment not supported"));
  }
}
