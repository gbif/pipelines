package org.gbif.pipelines.parsers.ws.client.metadata.contentful;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ContentServiceFactory {

  private static ContentService instance;

  public static synchronized ContentService create(String... hosts) {
    if (instance == null) {
      instance = new ContentService(hosts);
    }
    return instance;
  }
}
