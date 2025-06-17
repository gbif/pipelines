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
package org.gbif.pipelines.interpretation.spark;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import lombok.Data;

@Data
public class Config implements Serializable {
  @JsonProperty private String input;
  @JsonProperty private String output;
  @JsonProperty private String sparkRemote;
  @JsonProperty private String jarPath;
  @JsonProperty private String vocabularyApiUrl;
  @JsonProperty private String speciesMatchAPI = "https://api.gbif-uat.org/";
  @JsonProperty private Integer speciesMatchParallelism = 10;
  @JsonProperty private String geocodeAPI = "https://api.gbif.org/v1/";
  @JsonProperty private Integer geocodeParallelism = 10;
  @JsonProperty private List<String> checklistKeys;

  static Config fromFirstArg(String[] args) throws IOException {
    return new YAMLMapper().readValue(new File(args[0]), Config.class);
  }
}
