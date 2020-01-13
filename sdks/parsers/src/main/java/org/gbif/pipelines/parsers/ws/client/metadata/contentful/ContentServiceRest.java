package org.gbif.pipelines.parsers.ws.client.metadata.contentful;

public class ContentServiceRest {

  private final ContentService service;
  private static volatile ContentServiceRest instance;
  private static final Object MUTEX = new Object();

  private ContentServiceRest(String... hosts) {

    // create service
    service = new ContentService(hosts);
  }

  public static ContentServiceRest getInstance(String... hosts) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ContentServiceRest(hosts);
        }
      }
    }
    return instance;
  }

  public ContentService getService() {
    return service;
  }
}
