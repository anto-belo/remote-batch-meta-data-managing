package com.restbatch.batchservice.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Slf4j
public class JobExecutionDeserializer extends StdDeserializer<JobExecution> {

  public JobExecutionDeserializer() {
    this(null);
  }

  public JobExecutionDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  @SuppressWarnings("all")
  public JobExecution deserialize(JsonParser p, DeserializationContext ctx)
      throws IOException {
    ObjectMapper mapper = new MappingJackson2HttpMessageConverter().getObjectMapper();
    ObjectNode jobExecObj = p.getCodec().readTree(p);

    Long id = jobExecObj.get("id").asLong();
    String jobConfigurationName = jobExecObj.get("jobConfigurationName").asText();
    JobParameters jobParameters = mapper.treeToValue(jobExecObj.get("jobParameters"),
        JobParameters.class);
    Date startTime = parseTimestamp(jobExecObj.get("startTime").asText(null));
    Date endTime = parseTimestamp(jobExecObj.get("endTime").asText(null));
    BatchStatus status = BatchStatus.valueOf(jobExecObj.get("status").asText());
    String exitCode = jobExecObj.get("exitStatus").get("exitCode").asText();
    String exitDescription = jobExecObj.get("exitStatus").get("exitDescription").asText();
    Date createTime = parseTimestamp(jobExecObj.get("createTime").asText(null));
    Date lastUpdated = parseTimestamp(jobExecObj.get("endTime").asText(null));
    Integer version = jobExecObj.get("version").asInt();

    JobExecution jobExecution = new JobExecution(id, jobParameters, jobConfigurationName);

    jobExecution.setStartTime(startTime);
    jobExecution.setEndTime(endTime);
    jobExecution.setStatus(status);
    jobExecution.setExitStatus(new ExitStatus(exitCode, exitDescription));
    jobExecution.setCreateTime(createTime);
    jobExecution.setLastUpdated(lastUpdated);
    jobExecution.setVersion(version);
    return jobExecution;
  }

  private Timestamp parseTimestamp(String timestamp) {
    if (Objects.isNull(timestamp)) {
      return null;
    }

    String oldMilliseconds = timestamp.substring(timestamp.lastIndexOf('.') + 1,
        timestamp.length() - 6);
    String newMilliseconds = oldMilliseconds + "0".repeat(6 - oldMilliseconds.length());
    timestamp = timestamp.replace(oldMilliseconds, newMilliseconds);
    timestamp = timestamp.substring(0, 29) + timestamp.substring(30);

    try {
      DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ");
      return Timestamp.valueOf(LocalDateTime.parse(timestamp, dtf));
    } catch (DateTimeParseException e) {
      log.error("Cannot parse timestamp: {}. Reason: {}", timestamp, e.getMessage());
      return null;
    }
  }
}
