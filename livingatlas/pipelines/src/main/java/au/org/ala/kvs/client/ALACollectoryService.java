package au.org.ala.kvs.client;

import retrofit2.http.Path;

import java.io.Closeable;

/**
 * An interface for the collectory web services
 */
public interface ALACollectoryService extends Closeable {

  /**
   * Retrieve the details of a data resource.
   */
  ALACollectoryMetadata lookupDataResource(@Path("dataResourceUid") String dataResourceUid);

  /**
   * Lookup a Collection using institutionCode and collectionCode.
   */
  ALACollectionMatch lookupCodes(@Path("institutionCode") String institutionCode,
      @Path("collectionCode") String collectionCode);
}
