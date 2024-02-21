package au.org.ala.specieslists;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface SpeciesListServiceV2 extends SpeciesListService {
  @GET("/speciesList/?isAuthoritative=true&pageSize=1000")
  Call<ListSearchResponse> getAuthoritativeLists();

  @GET("/download/{dataResourceUid}")
  Call<ResponseBody> downloadList(@Path("dataResourceUid") String dataResourceUid);
}
