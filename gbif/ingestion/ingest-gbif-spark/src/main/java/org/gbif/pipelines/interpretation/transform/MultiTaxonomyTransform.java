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
package org.gbif.pipelines.interpretation.transform;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.interpreters.Interpretation;
import org.gbif.pipelines.core.interpreters.core.MultiTaxonomyInterpreter;
import org.gbif.pipelines.interpretation.transform.utils.MultiTaxonomyKVSFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.rest.client.species.NameUsageMatchResponse;

@Slf4j
@Builder
public class MultiTaxonomyTransform implements Serializable {

  private String nameUsageMatchApiUrl;
  private List<String> checklistKeys;

  public Optional<MultiTaxonRecord> convert(ExtendedRecord source) {

    KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore =
        MultiTaxonomyKVSFactory.getKvStore(nameUsageMatchApiUrl);

    return Interpretation.from(source)
        .to(
            er ->
                MultiTaxonRecord.newBuilder()
                    .setId(er.getId())
                    .setCreated(Instant.now().toEpochMilli())
                    .build())
        .when(er -> !er.getCoreTerms().isEmpty())
        .via(MultiTaxonomyInterpreter.interpretMultiTaxonomy(kvStore, checklistKeys))
        .getOfNullable();
  }
}
