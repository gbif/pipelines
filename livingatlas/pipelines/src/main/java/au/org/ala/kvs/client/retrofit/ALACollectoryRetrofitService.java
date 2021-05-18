package au.org.ala.kvs.client.retrofit;

import au.org.ala.kvs.client.ALACollectionMatch;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.kvs.client.EntityReference;
import java.util.List;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

/** Collectory retrofit web service interface */
public interface ALACollectoryRetrofitService {

  @GET("dataResource/{dataResourceUid}")
  Call<ALACollectoryMetadata> lookupDataResource(@Path("dataResourceUid") String dataResourceUid);

  @GET("lookup/inst/{institutionCode}/coll/{collectionCode}")
  Call<ALACollectionMatch> lookupCodes(
      @Path("institutionCode") String institutionCode,
      @Path("collectionCode") String collectionCode);

  @GET("dataResource")
  Call<List<EntityReference>> lookupDataResources();
}
