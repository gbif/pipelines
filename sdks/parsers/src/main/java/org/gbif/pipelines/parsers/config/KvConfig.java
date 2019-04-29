package org.gbif.pipelines.parsers.config;

import org.aeonbits.owner.Config;

public interface KvConfig extends Config {

  @Key("gbif.api.url")
  String getBasePath();

  @Key("zookeeper.url")
  String getZookeeperUrl();

  @Key("taxonomy.ws.timeout")
  @DefaultValue("60")
  long getTaxonomyTimeout();

  @Key("taxonomy.ws.cache.sizeMb")
  @DefaultValue("64")
  long getTaxonomyCacheSizeMb();

  @Key("taxonomy.tableName")
  String getTaxonomyTableName();

  @Key("taxonomy.numOfKeyBuckets")
  @DefaultValue("10")
  int getTaxonomyNumOfKeyBuckets();

  @Key("taxonomy.restOnly")
  @DefaultValue("false")
  boolean getTaxonomyRestOnly();

  @Key("geocode.ws.timeout")
  @DefaultValue("60")
  long getGeocodeTimeout();

  @Key("geocode.ws.cache.sizeMb")
  @DefaultValue("64")
  long getGeocodeCacheSizeMb();

  @Key("geocode.tableName")
  String getGeocodeTableName();

  @Key("geocode.numOfKeyBuckets")
  @DefaultValue("10")
  int getGeocodeNumOfKeyBuckets();

  @Key("geocode.restOnly")
  @DefaultValue("false")
  boolean getGeocodeRestOnly();

  @Key("australia.spatial.ws.timeout")
  @DefaultValue("60")
  long getAustraliaTimeout();

  @Key("australia.spatial.ws.cache.sizeMb")
  @DefaultValue("64")
  long getAustraliaCacheSizeMb();

  @Key("australia.spatial.tableName")
  String getAustraliaTableName();

  @Key("australia.spatial.numOfKeyBuckets")
  @DefaultValue("20")
  int getAustraliaNumOfKeyBuckets();
}
