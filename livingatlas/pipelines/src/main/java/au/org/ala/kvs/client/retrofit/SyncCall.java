/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.org.ala.kvs.client.retrofit;

/* Note: Added temporarily to avoid breaking changes in the codebase, as was removed by:
  https://github.com/gbif/key-value-store/commit/1c97259aa17c66d07d4b9d69929a84544976496e#diff-6f15aebebad4bde812026962cbfab559d8313ade72851ddc98f66a430f2ba035
*/

import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClientException;
import retrofit2.Call;
import retrofit2.HttpException;
import retrofit2.Response;

/** Utility class to perform synchronous call on Retrofit services. */
public class SyncCall {

  private static final Logger LOG = LoggerFactory.getLogger(SyncCall.class);

  /** Private constructor. */
  private SyncCall() {
    // DO NOTHING
  }

  /**
   * Performs a synchronous call to {@link Call} instance.
   *
   * @param call to be executed
   * @param <T> content of the response object
   * @return the content of the response, throws an {@link HttpException} in case of error
   */
  public static <T> T syncCall(Call<T> call) {
    try {
      Response<T> response = call.execute();
      if (response.isSuccessful() && response.body() != null) {
        return response.body();
      }
      LOG.error("Service responded with an error {}", response);
      throw new HttpException(response); // Propagates the failed response
    } catch (IOException ex) {
      throw new RestClientException("Error executing call", ex);
    }
  }

  /**
   * Performs a synchronous call to {@link Call} instance that can return a null response.
   *
   * @param call to be executed
   * @param <T> content of the response object
   * @return the content of the response, throws an {@link HttpException} in case of error
   */
  public static <T> Optional<T> nullableSyncCall(Call<T> call) {
    try {
      Response<T> response = call.execute();
      if (response.isSuccessful()) {
        return Optional.ofNullable(response.body());
      }
      LOG.error("Service responded with an error {}", response);
      throw new HttpException(response); // Propagates the failed response
    } catch (IOException ex) {
      throw new RestClientException("Error executing call", ex);
    }
  }
}
