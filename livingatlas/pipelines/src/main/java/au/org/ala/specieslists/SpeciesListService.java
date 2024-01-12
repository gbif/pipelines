package au.org.ala.specieslists;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface SpeciesListService {

  @GET("/speciesList/?isAuthoritative=true&max=1000")
  Call<ListSearchResponse> getAuthoritativeLists();

  @GET("/download/{dataResourceUid}")
  Call<ResponseBody> downloadList(@Path("dataResourceUid") String dataResourceUid);
}
