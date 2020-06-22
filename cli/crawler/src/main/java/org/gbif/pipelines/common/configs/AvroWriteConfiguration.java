package org.gbif.pipelines.common.configs;

import org.apache.avro.file.CodecFactory;

import com.beust.jcommander.Parameter;
import javax.validation.constraints.NotNull;
import lombok.ToString;

@ToString
public class AvroWriteConfiguration {

  @Parameter(names = "--compression-type")
  @NotNull
  public String compressionType = CodecFactory.snappyCodec().toString();

  @Parameter(names = "--sync-interval")
  @NotNull
  public int syncInterval = 2 * 1024 * 1024;
}
