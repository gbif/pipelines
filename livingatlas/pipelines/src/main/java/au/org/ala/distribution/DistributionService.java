package au.org.ala.distribution;

import java.util.List;
import java.util.Map;
import retrofit2.Call;
import retrofit2.http.*;

/** Simple client to the ALA sampling service. */
public interface DistributionService {

  /** Return expert distribution layers in the ALA spatial portal */
  @GET("distribution/")
  Call<List<DistributionLayer>> getLayers();

  @GET("distribution/lsids/{id}")
  Call<List<DistributionLayer>> getLayersByLsid(
      @Path("id") String id, @Query("nowkt") String nowkt);

  /**
   * @param lsid
   * @param points Map<uuid, <decimalLatitude,decimalLongitude>>
   * @return
   */
  @POST("distribution/outliers/{id}")
  Call<Map<String, Double>> outliers(
      @Path("id") String lsid, @Body Map<String, Map<String, Double>> points);
}
