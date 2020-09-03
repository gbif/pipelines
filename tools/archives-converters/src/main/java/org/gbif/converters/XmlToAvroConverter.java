package org.gbif.converters;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.converter.ConverterToVerbatim;
import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.parser.xml.ExtendedRecordConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Converts ABCD/etc archive into {@link ExtendedRecord} AVRO file */
@Slf4j
@NoArgsConstructor(staticName = "create")
public class XmlToAvroConverter extends ConverterToVerbatim {

  private ExecutorService executor = Executors.newWorkStealingPool();

  /** @param executor to use provided ExecutorService */
  public XmlToAvroConverter executor(ExecutorService executor) {
    this.executor = executor;
    return this;
  }

  /** @param xmlReaderParallelism number of threads for reader */
  public XmlToAvroConverter xmlReaderParallelism(int xmlReaderParallelism) {
    this.executor = Executors.newFixedThreadPool(xmlReaderParallelism);
    return this;
  }

  public static void main(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify input and output paths");
    }
    String inputPath = args[0];
    String outputPath = args[1];
    boolean isFileCreated =
        XmlToAvroConverter.create().inputPath(inputPath).outputPath(outputPath).convert();
    log.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /**
   * Converts ABCD/etc archive into {@link ExtendedRecord} AVRO file
   *
   * @param inputPath Path to DWCA file
   * @param dataFileWriter AVRO data writer for {@link ExtendedRecord}
   */
  @Override
  public long convert(Path inputPath, SyncDataFileWriter<ExtendedRecord> dataFileWriter) {
    return ExtendedRecordConverter.create(executor).toAvro(inputPath.toString(), dataFileWriter);
  }
}
