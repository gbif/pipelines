package au.org.ala.kvs.client;

import java.io.Closeable;
import java.util.List;
import retrofit2.http.Path;

/** An interface for the collectory web services */
public interface ALACollectoryService extends Closeable {

  /** Retrieve the details of a data resource. */
  ALACollectoryMetadata lookupDataResource(@Path("dataResourceUid") String dataResourceUid);

  /** Lookup a Collection using institutionCode and collectionCode. */
  ALACollectionMatch lookupCodes(
      @Path("institutionCode") String institutionCode,
      @Path("collectionCode") String collectionCode);

  /** List the data resources */
  List<EntityReference> listDataResources() throws Exception;
}
