/**
 * Copyright (C) 2020 Atlas of Living Australia All Rights Reserved. The contents of this file are
 * subject to the Mozilla Public License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * http://www.mozilla.org/MPL/ Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for the specific
 * language governing rights and limitations under the License.
 */
package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.common.ALARecordTypes.ALA_UUID;

import java.util.Optional;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.transforms.Transform;

public class ALAUUIDTransform extends Transform<ExtendedRecord, ALAUUIDRecord> {

  private ALAUUIDTransform() {
    super(ALAUUIDRecord.class, ALA_UUID, ALAUUIDTransform.class.getName(), "alaUuidCount");
  }

  public static ALAUUIDTransform create() {
    return new ALAUUIDTransform();
  }

  /** Maps {@link ALATaxonRecord} to key value, where key is {@link TaxonRecord#getId} */
  public MapElements<ALAUUIDRecord, KV<String, ALAUUIDRecord>> toKv() {
    return MapElements.into(new TypeDescriptor<KV<String, ALAUUIDRecord>>() {})
        .via((ALAUUIDRecord tr) -> KV.of(tr.getId(), tr));
  }

  @Override
  public Optional<ALAUUIDRecord> convert(ExtendedRecord extendedRecord) {
    throw new IllegalArgumentException("Method is not implemented!");
  }
}
