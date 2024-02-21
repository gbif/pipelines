package au.org.ala.specieslists;

import okhttp3.ResponseBody;
import retrofit2.Call;

public interface SpeciesListService {

  Call<ListSearchResponse> getAuthoritativeLists();

  Call<ResponseBody> downloadList(String dataResourceUid);
}
