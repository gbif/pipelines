package au.org.ala.pipelines.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

@Slf4j
public class VersionInfo {
  public static void print() {
    try {
      InputStream is = VersionInfo.class.getResourceAsStream("/git.properties");
      BufferedReader streamReader =
          new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
      JSONParser parser = new JSONParser();
      JSONObject info = (JSONObject) parser.parse(streamReader);
      log.info("You are running build version: " + info.get("git.build.version"));
      log.info("Build time: " + info.get("git.build.time"));
      log.info("Commit version: " + info.get("git.commit.id"));
      log.info("Commit message: " + info.get("git.commit.message.full"));
      log.debug("Details: " + info.toJSONString());
    } catch (Exception e) {
      log.warn("Cannot load git version information!");
    }
  }
}
