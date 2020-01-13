package org.gbif.pipelines.parsers.ws.client.metadata.contentful;

public class ContentServiceFactory {

  private final ContentService service;
  private static volatile ContentServiceFactory instance;
  private static final Object MUTEX = new Object();

  private ContentServiceFactory(String... hosts) {

    // create service
    service = new ContentService(hosts);
  }

  public static ContentServiceFactory getInstance(String... hosts) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ContentServiceFactory(hosts);
        }
      }
    }
    return instance;
  }

  public ContentService getService() {
    return service;
  }
}
