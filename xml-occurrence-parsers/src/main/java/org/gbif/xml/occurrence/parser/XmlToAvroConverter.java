package org.gbif.xml.occurrence.parser;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ConverterTask;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ParserFileUtils;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.SyncDataFileWriter;
import org.gbif.xml.occurrence.parser.parsing.validators.UniquenessValidator;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parsing xml response files or tar.xz archive and convert to ExtendedRecord avro file
 */
public class XmlToAvroConverter {

  private static final Logger LOG = LoggerFactory.getLogger(XmlToAvroConverter.class);
  private static final String FILE_PREFIX = ".response";
  private final String inputpath;

  /**
   * read directory having xml files
   */
  public static XmlToAvroConverter readFrom(String xmlPath) {
    return new XmlToAvroConverter(xmlPath);
  }

  private XmlToAvroConverter(String inputpath) {
    this.inputpath = inputpath;
  }

  /**
   * write to avro as extended record in the provided outputstream and configurations (sync interval and compression codec)
   */
  public void writeWithConfiguration(OutputStream os, int syncInterval, CodecFactory codec) {
    convertFromXML(this.inputpath, os, syncInterval, codec);
  }

  /**
   * @param inputPath    path to directory with response files or a tar.xz archive
   * @param outputStream output stream to support any file system
   */
  private void convertFromXML(String inputPath, OutputStream outputStream, int syncInterval, CodecFactory codec) {

    if (Strings.isNullOrEmpty(inputPath) || Objects.isNull(outputStream)) {
      throw new ParsingException("Input or output stream must not be empty or null!");
    }

    File inputFile = ParserFileUtils.uncompressAndGetInputFile(inputPath);
    Schema schema = ExtendedRecord.getClassSchema();

    try (DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));
         Stream<Path> walk = Files.walk(inputFile.toPath());
         UniquenessValidator validator = UniquenessValidator.getNewInstance()) {
      dataFileWriter.setSyncInterval(syncInterval);
      dataFileWriter.setCodec(codec);
      dataFileWriter.setFlushOnEveryBlock(false);
      dataFileWriter.create(schema, outputStream);

      // Class with sync method to avoid problem with writing
      SyncDataFileWriter syncWriter = new SyncDataFileWriter(dataFileWriter);

      // Run async process - read a file, convert to ExtendedRecord and write to avro
      CompletableFuture[] futures = walk.filter(x -> x.toFile().isFile() && x.toString().endsWith(FILE_PREFIX))
        .map(Path::toFile)
        .map(file -> CompletableFuture.runAsync(new ConverterTask(file, syncWriter, validator)))
        .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();
      dataFileWriter.flush();

    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new ParsingException(ex);
    }
  }

}