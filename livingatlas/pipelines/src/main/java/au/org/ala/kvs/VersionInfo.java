package au.org.ala.kvs;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VersionInfo {

      /*"git.build.host" : "macropod-bm",
      "git.build.time" : "2020-10-21T15:49:36+1100",
      "git.build.user.email" : "qifeng.bai@csiro.au",
      "git.build.user.name" : "Qifeng",
      "git.build.version" : "2.5.5-SNAPSHOT",
      "git.closest.tag.commit.count" : "206",
      "git.closest.tag.name" : "pipelines-parent-2.5.4",
      "git.commit.id" : "5248fae2af633aa99bfe17d5f487581500050d35",
      "git.commit.id.abbrev" : "5248fae",
      "git.commit.id.describe" : "pipelines-parent-2.5.4-206-g5248fae-dirty",
      "git.commit.id.describe-short" : "pipelines-parent-2.5.4-206-dirty",
      "git.commit.message.full" : "build fix for ala-dev branch, parser SNAPSHOT",
      "git.commit.message.short" : "build fix for ala-dev branch, parser SNAPSHOT",
      "git.commit.time" : "2020-10-08T19:19:03+1100",
      "git.commit.user.email" : "djtfmartin@users.noreply.github.com",
      "git.commit.user.name" : "Dave Martin",
      "git.dirty" : "true",
      "git.remote.origin.url" : "https://github.com/gbif/pipelines.git",
      "git.tags" : ""
*/
  public static void print(){
    try{
      InputStream is = VersionInfo.class.getResourceAsStream("/git.properties");
      BufferedReader streamReader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
      JSONParser parser = new JSONParser();
      JSONObject info = (JSONObject)parser.parse(streamReader);
      log.info(info.toJSONString());

    }catch(Exception e){
      log.warn("Cannot load git version information!");
    }
  }

}
