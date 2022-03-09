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

  /**
   * @param id the species id MUST be URLEncoded
   * @param nowkt
   * @return
   */
  @GET("distribution/lsids/{id}")
  Call<List<DistributionLayer>> getLayersByLsid(
      @Path(value = "id") String id, @Query("nowkt") String nowkt);

  /**
   * @param id the species id MUST be URLEncoded
   * @param points Map<uuid, <decimalLatitude,decimalLongitude>>
   * @return
   */
  @POST("distribution/outliers/{id}")
  Call<Map<String, Double>> outliers(
      @Path(value = "id") String id, @Body Map<String, Map<String, Double>> points);
}
