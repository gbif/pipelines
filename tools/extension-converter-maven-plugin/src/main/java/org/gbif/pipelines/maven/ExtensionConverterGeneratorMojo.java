package org.gbif.pipelines.maven;

import static java.nio.charset.StandardCharsets.UTF_8;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.gbif.dwc.digester.ThesaurusHandlingRule;
import org.gbif.dwc.extensions.Extension;
import org.gbif.dwc.extensions.ExtensionFactory;
import org.gbif.dwc.extensions.VocabulariesManager;
import org.gbif.dwc.extensions.Vocabulary;
import org.gbif.dwc.xml.SAXUtils;
import org.gbif.pipelines.maven.ExtensionPojo.Setter;

@Mojo(name = "extensiongeneration", defaultPhase = LifecyclePhase.GENERATE_SOURCES)
public class ExtensionConverterGeneratorMojo extends AbstractMojo {

  private static final String POSTFIX_CLASS_NAME = "Converter.java";

  @Parameter(property = "extensiongeneration.extensions")
  private List<String> extensions;

  @Parameter(property = "extensiongeneration.pathToWrite")
  private String pathToWrite;

  @Parameter(property = "extensiongeneration.packagePath")
  private String packagePath;

  @Parameter(property = "extensiongeneration.avroNamespace")
  private String avroNamespace;

  @Override
  public void execute() throws MojoExecutionException {

    try {

      String finalPath = String.join("/", pathToWrite, packagePath.replace('.', '/'));
      Files.createDirectories(Paths.get(finalPath));
      Configuration cfg = createConfig();

      for (String extension : extensions) {

        String[] ext = extension.split(",");

        URL url = new URL(ext[1]);
        String name = ext[0];

        Path classPath = Paths.get(finalPath, name + POSTFIX_CLASS_NAME);
        if (!Files.exists(classPath)) {
          Extension dwcExt = getExtension(url);

          List<Setter> setters =
              dwcExt.getProperties().stream()
                  .map(e -> new Setter(e.getQualname(), normalizeName(e.getName())))
                  .collect(Collectors.toList());

          ExtensionPojo extPojo =
              new ExtensionPojo(
                  name, avroNamespace, packagePath, dwcExt.getRowType().qualifiedName(), setters);

          Template temp =
              new Template("table-converter", new StringReader(Templates.TABLE_CONVERTER), cfg);

          Writer out = new OutputStreamWriter(new FileOutputStream(classPath.toFile()));
          temp.process(extPojo, out);

          getLog().info("DWC extension java converter class is generated - " + classPath);
        }
      }

    } catch (Exception ex) {
      throw new MojoExecutionException(ex.getMessage());
    }
  }

  public void setExtensions(List<String> extensions) {
    this.extensions = extensions;
  }

  public void setPathToWrite(String pathToWrite) {
    this.pathToWrite = pathToWrite;
  }

  public void setAvroNamespace(String avroNamespace) {
    this.avroNamespace = avroNamespace;
  }

  public void setPackagePath(String packagePath) {
    this.packagePath = packagePath;
  }
  /** Create Freemarker config */
  private Configuration createConfig() {
    Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
    cfg.setDefaultEncoding(UTF_8.toString());
    cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    cfg.setLogTemplateExceptions(false);
    cfg.setWrapUncheckedExceptions(true);
    cfg.setFallbackOnNullLoopVariable(false);
    cfg.setTabSize(1);
    return cfg;
  }

  /** Read DWC extension from URL */
  private Extension getExtension(URL url) throws Exception {
    ThesaurusHandlingRule thr = new ThesaurusHandlingRule(new EmptyVocabulariesManager());
    ExtensionFactory factory = new ExtensionFactory(thr, SAXUtils.getNsAwareSaxParserFactory());
    return factory.build(url.openStream(), url, false);
  }

  /** Normalize name and retun result like - Normalizename */
  protected String normalizeName(String name) {
    String replace =
        name.toLowerCase().trim().replaceAll("-", "").replaceAll("_", "").replace("class", "clazz");
    return replace.substring(0, 1).toUpperCase() + replace.substring(1);
  }

  private static class EmptyVocabulariesManager implements VocabulariesManager {

    @Override
    public Vocabulary get(String uri) {
      return null;
    }

    @Override
    public Vocabulary get(URL url) {
      return null;
    }

    @Override
    public Map<String, String> getI18nVocab(String uri, String lang) {
      return null;
    }

    @Override
    public List<Vocabulary> list() {
      return null;
    }
  }
}
