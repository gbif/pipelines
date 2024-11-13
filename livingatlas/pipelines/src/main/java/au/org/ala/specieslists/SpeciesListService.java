package au.org.ala.specieslists;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;

public interface SpeciesListService {

  @GET("ws/speciesList?isAuthoritative=eq:true&max=1000")
  Call<ListSearchResponse> getAuthoritativeLists();

  @GET("speciesListItem/downloadList/{dataResourceUid}?fetch=%7BkvpValues%3Dselect%7")
  Call<ResponseBody> downloadList(@Path("dataResourceUid") String dataResourceUid);
}
