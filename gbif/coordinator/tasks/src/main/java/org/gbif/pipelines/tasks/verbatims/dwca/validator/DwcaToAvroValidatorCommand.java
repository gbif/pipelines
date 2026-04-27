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
package org.gbif.pipelines.tasks.verbatims.dwca.validator;

import com.google.common.util.concurrent.Service;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

/**
 * Entry class for cli command, to start service to convert downloaded DwCA Archive to Avro. This
 * command starts a service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesValidatorDwcaMessage } and perform conversion.
 * Similar to {@link org.gbif.pipelines.tasks.verbatims.dwca.DwcaToAvroCommand} but for Pipelines
 * Validator.
 */
@MetaInfServices(Command.class)
public class DwcaToAvroValidatorCommand extends ServiceCommand {

  private final DwcaToAvroValidatorConfiguration config = new DwcaToAvroValidatorConfiguration();

  public DwcaToAvroValidatorCommand() {
    super("pipelines-validator-verbatim-to-avro-from-dwca");
  }

  @Override
  protected Service getService() {
    return new DwcaToAvroValidatorService(config);
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }
}
