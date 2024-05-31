package uk.org.nbn.accesscontrol;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import uk.org.nbn.dto.DataResourceNbn;

import java.util.concurrent.ConcurrentHashMap;

public class DataResourceNbnCache {

    private static DataResourceNbnCache instance;
    private final ConcurrentHashMap<String, DataResourceNbn> cache;

    private DataResourceNbnCache() {
        cache = new ConcurrentHashMap<>();
  }

    public static synchronized DataResourceNbnCache getInstance() {
        if (instance == null) {
            instance = new DataResourceNbnCache();
        }
        return instance;
    }

    public DataResourceNbn getDataResourceNbn(String uid){
        if (true) return new DataResourceNbn(uid,10000,10000,false);
        return cache.computeIfAbsent(uid, key -> {
            try {
//  TODO NBN              String url = Config.registryUrl() + "/accessControl/dataResourceNbn/" + uid+"?api_key=" + Config.collectoryApiKey();
                String url = "https://registry.legacy.nbnatlas.org/ws/accessControl/dataResourceNbn/" + uid+"?api_key=fd051578-4a55-4b4f-864d-c71923e2d029";
                OkHttpClient client = new OkHttpClient();

                Request request = new Request.Builder()
                        .url(url)
                        .build();

                try (Response response = client.newCall(request).execute()) {
                    if (response.code() == 404) {
                        return null;
                    } else if (!response.isSuccessful()) {
                        System.out.println("Request failed with status code: " + response.code()+" api call:"+url);
                        return null;
                    } else {
                        String json = response.body().string();
                        ObjectMapper objectMapper = new ObjectMapper();
                        objectMapper.registerModule(new DefaultScalaModule());
                        DataResourceNbn dataResourceNbn = objectMapper.readValue(json, DataResourceNbn.class);
                        return dataResourceNbn;
                    }
                }
            } catch (Exception e) {
                throw new Error("IOException getting NBN's DataResource Metadata", e);
            }
        });
    }


}

