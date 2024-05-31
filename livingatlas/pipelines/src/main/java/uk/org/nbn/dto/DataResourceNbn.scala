package uk.org.nbn.dto

import com.fasterxml.jackson.annotation.JsonProperty

import scala.beans.BeanProperty

class DataResourceNbn(
                          @JsonProperty @BeanProperty var dataResourceUid:String,
                          @JsonProperty @BeanProperty var publicResolution:Integer,
                          @JsonProperty @BeanProperty var publicResolutionToBeApplied:Integer,
                          @JsonProperty @BeanProperty var needToReload:Boolean
                        )
{

}
