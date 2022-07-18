package com.restbatch.batchservice.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.springframework.batch.core.JobInstance;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public class JobInstanceDeserializer extends StdDeserializer<JobInstance> {

  public JobInstanceDeserializer() {
    this(null);
  }

  public JobInstanceDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public JobInstance deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
    ObjectNode jobInstObj = p.getCodec().readTree(p);

    Long id = jobInstObj.get("id").asLong();
    String name = jobInstObj.get("jobName").asText();
    Integer version = jobInstObj.get("version").asInt();

    JobInstance jobInstance = new JobInstance(id, name);
    jobInstance.setVersion(version);
    return jobInstance;
  }
}
