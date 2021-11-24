package au.org.ala.distribution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

@Slf4j
public class DistributionServiceImpl implements Serializable {

  private Retrofit retrofit;
  private DistributionService service;

  private DistributionServiceImpl(String baseUrl) {
    ObjectMapper om =
        new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    JacksonConverterFactory jcf = JacksonConverterFactory.create(om);
    retrofit = new Retrofit.Builder().baseUrl(baseUrl).addConverterFactory(jcf).build();
    service = retrofit.create(DistributionService.class);
  }

  public static DistributionServiceImpl init(String baseUrl) {
    // set up sampling service
    return new DistributionServiceImpl(baseUrl);
  }

  public List<DistributionLayer> getLayers() throws IOException, ExpertDistributionException {
    // Response<List<DistributionLayer>> response =
    // distributionService.getLayersByLsid("urn:lsid:biodiversity.org.au:afd.taxon:4f3a5260-4f39-4393-a644-4d05b1c45f92", "false").execute();
    Response<List<au.org.ala.distribution.DistributionLayer>> response =
        service.getLayers().execute();
    int code = response.code();
    if (code >= 200 && code < 300) {
      List<au.org.ala.distribution.DistributionLayer> layers = response.body();
      return layers;
    } else {
      errorHandler(code, response);
      return null;
    }
  }

  public List<DistributionLayer> findLayersByLsid(String lsid)
      throws IOException, ExpertDistributionException {
    Response<List<DistributionLayer>> response = service.getLayersByLsid(lsid, "false").execute();
    int code = response.code();
    if (code >= 200 && code < 300) {
      List<au.org.ala.distribution.DistributionLayer> layers = response.body();
      return layers;
    } else {
      errorHandler(code, response);
      return null;
    }
  }

  public Map<String, Double> outliers(String lsid, Map<String, Map<String, Double>> points)
      throws IOException, ExpertDistributionException {
    Response<Map<String, Double>> response = service.outliers(lsid, points).execute();
    int code = response.code();
    if (code >= 200 && code < 300) {
      Map<String, Double> results = response.body();
      return results;
    } else {
      errorHandler(code, response);
      return null;
    }
  }

  /**
   * Todo Handle error information Need to sync with Spatial Service
   *
   * @param code
   * @param response
   * @throws IOException
   * @throws ExpertDistributionException
   */
  private void errorHandler(int code, Response<?> response)
      throws IOException, ExpertDistributionException {
    String errorBody = response.errorBody().string();
    if (code >= 400 && code < 500) {
      throw new ExpertDistributionException(errorBody);
    } else {
      throw new RuntimeException(errorBody);
    }
  }
}
