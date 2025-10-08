package au.org.ala.images;

import okhttp3.MultipartBody;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.*;

/** An interface representing ALA's image service. */
public interface ImageService {

  @Multipart
  @POST("batch/upload")
  Call<BatchUploadResponse> upload(
      @Part("dataResourceUid") RequestBody dataResourceUid, @Part MultipartBody.Part file);

  @GET("ws/exportDataset/{dataResourceUid}")
  Call<ResponseBody> downloadMappingFile(@Path("dataResourceUid") String dataResourceUid);
}
