package org.gbif.pipelines.keygen.config;

import org.aeonbits.owner.Config;

public interface KeygenConfig extends Config {

  @Key("keygen.table.occ")
  String getOccTable();

  @Key("keygen.table.counter")
  String getCounterTable();

  @Key("keygen.table.lookup")
  String getLookupTable();

  @Key("zookeeper.url")
  String getHbaseZk();
}
