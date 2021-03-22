package au.org.ala.sampling;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import retrofit2.Call;
import retrofit2.http.*;

/** Simple client to the ALA sampling service. */
public interface SamplingService {

  /** Return an inventory of fields in the ALA spatial portal */
  @GET("fields")
  Call<List<Field>> getFields();

  /** Return an inventory of layers in the ALA spatial portal */
  @GET("layers")
  Call<List<Layer>> getLayers();

  /** Return an inventory of layers in the ALA spatial portal */
  @GET("intersect/batch/{id}")
  Call<BatchStatus> getBatchStatus(@Path("id") String id);

  /**
   * Trigger a job to run the intersection and return the job key to poll.
   *
   * @param layerIds The layers of interest in comma separated form
   * @param coordinatePairs The coordinates in lat,lng,lat,lng... format
   * @return The batch submited
   */
  @FormUrlEncoded
  @POST("intersect/batch")
  Call<Batch> submitIntersectBatch(
      @retrofit2.http.Field("fids") String layerIds,
      @retrofit2.http.Field("points") String coordinatePairs);

  @Getter
  @Setter
  @JsonIgnoreProperties(ignoreUnknown = true)
  class Batch {
    private String batchId;
  }

  @Getter
  @Setter
  @JsonIgnoreProperties(ignoreUnknown = true)
  class BatchStatus {
    private String status;
    private String downloadUrl;
  }
}
