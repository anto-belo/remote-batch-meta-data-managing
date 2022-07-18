package com.restbatch.batchservice.dao;

import com.restbatch.batchservice.client.ExecutionContextDaoClient;
import com.restbatch.batchservice.dto.SerializedContextDto;
import com.restbatch.batchservice.dto.SerializedContextsDto;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Setter;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.serializer.Serializer;
import org.springframework.util.Assert;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public class RemoteExecutionContextDao extends AbstractBatchMetaDataDao
    implements ExecutionContextDao {

  private static final int DEFAULT_MAX_VARCHAR_LENGTH = 2500;

  @Setter
  private ExecutionContextDaoClient client;
  private int shortContextLength = DEFAULT_MAX_VARCHAR_LENGTH;
  private ExecutionContextSerializer serializer;

  /**
   * Setter for {@link Serializer} implementation
   *
   * @param serializer {@link ExecutionContextSerializer} instance to use.
   */
  public void setSerializer(ExecutionContextSerializer serializer) {
    Assert.notNull(serializer, "Serializer must not be null");
    this.serializer = serializer;
  }

  /**
   * The maximum size that an execution context can have and still be stored completely in short
   * form in the column <code>SHORT_CONTEXT</code>. Anything longer than this will overflow into
   * large-object storage, and the first part only will be retained in the short form for
   * readability. Default value is 2500. Clients using multi-bytes charsets on the database server
   * may need to reduce this value to as little as half the value of the column size.
   *
   * @param shortContextLength int max length of the short context.
   */
  public void setShortContextLength(int shortContextLength) {
    this.shortContextLength = shortContextLength;
  }

  @Override
  public ExecutionContext getExecutionContext(JobExecution jobExecution) {
    Long executionId = jobExecution.getId();
    Assert.notNull(executionId, "ExecutionId must not be null.");

    ExecutionContext ctx = client.getJobExecutionContext(executionId);
    return ctx.containsKey("_CTX") ? deserializeContext(ctx.getString("_CTX")) : ctx;
  }

  @Override
  public ExecutionContext getExecutionContext(StepExecution stepExecution) {
    Long executionId = stepExecution.getId();
    Assert.notNull(executionId, "ExecutionId must not be null.");

    ExecutionContext ctx = client.getStepExecutionContext(executionId);
    return ctx.containsKey("_CTX") ? deserializeContext(ctx.getString("_CTX")) : ctx;
  }

  @Override
  public void updateExecutionContext(final JobExecution jobExecution) {
    Long executionId = jobExecution.getId();
    ExecutionContext executionContext = jobExecution.getExecutionContext();
    Assert.notNull(executionId, "ExecutionId must not be null.");
    Assert.notNull(executionContext, "The ExecutionContext must not be null.");

    String serializedContext = serializeContext(executionContext);

    persistSerializedContext(executionId, serializedContext, "updateJobExecutionContext");
  }

  @Override
  public void updateExecutionContext(final StepExecution stepExecution) {
    // Attempt to prevent concurrent modification errors by blocking here if
    // someone is already trying to do it.
    synchronized (stepExecution) {
      Long executionId = stepExecution.getId();
      ExecutionContext executionContext = stepExecution.getExecutionContext();
      Assert.notNull(executionId, "ExecutionId must not be null.");
      Assert.notNull(executionContext, "The ExecutionContext must not be null.");

      String serializedContext = serializeContext(executionContext);

      persistSerializedContext(executionId, serializedContext, "updateStepExecutionContext");
    }
  }

  @Override
  public void saveExecutionContext(JobExecution jobExecution) {
    Long executionId = jobExecution.getId();
    ExecutionContext executionContext = jobExecution.getExecutionContext();
    Assert.notNull(executionId, "ExecutionId must not be null.");
    Assert.notNull(executionContext, "The ExecutionContext must not be null.");

    String serializedContext = serializeContext(executionContext);

    persistSerializedContext(executionId, serializedContext, "insertJobExecutionContext");
  }

  @Override
  public void saveExecutionContext(StepExecution stepExecution) {
    Long executionId = stepExecution.getId();
    ExecutionContext executionContext = stepExecution.getExecutionContext();
    Assert.notNull(executionId, "ExecutionId must not be null.");
    Assert.notNull(executionContext, "The ExecutionContext must not be null.");

    String serializedContext = serializeContext(executionContext);

    persistSerializedContext(executionId, serializedContext, "insertStepExecutionContext");
  }

  @Override
  public void saveExecutionContexts(Collection<StepExecution> stepExecutions) {
    Assert.notNull(stepExecutions, "Attempt to save an null collection of step executions");
    Map<Long, String> serializedContexts = new HashMap<>(stepExecutions.size());
    for (StepExecution stepExecution : stepExecutions) {
      Long executionId = stepExecution.getId();
      ExecutionContext executionContext = stepExecution.getExecutionContext();
      Assert.notNull(executionId, "ExecutionId must not be null.");
      Assert.notNull(executionContext, "The ExecutionContext must not be null.");
      serializedContexts.put(executionId, serializeContext(executionContext));
    }
    persistSerializedContexts(serializedContexts, "insertStepExecutionContext");
  }

  @Override
  public void afterPropertiesSet() {
    Assert.state(client != null, "ExecutionContextDaoClient is required");
    Assert.state(serializer != null, "ExecutionContextSerializer is required");
  }

  private void persistSerializedContext(final Long executionId, String serializedContext,
      String sqlType) {
    final String shortContext;
    final String longContext;
    if (serializedContext.length() > shortContextLength) {
      // Overestimate length of ellipsis to be on the safe side with
      // 2-byte chars
      shortContext = serializedContext.substring(0, shortContextLength - 8) + " ...";
      longContext = serializedContext;
    } else {
      shortContext = serializedContext;
      longContext = null;
    }

    client.persistSerializedContext(
        SerializedContextDto.builder()
            .sqlType(sqlType)
            .shortContext(shortContext)
            .longContext(longContext)
            .clobTypeToUse(getClobTypeToUse())
            .executionId(executionId)
            .build());
  }

  private void persistSerializedContexts(final Map<Long, String> serializedContexts,
      String sqlType) {
    client.persistSerializedContexts(
        SerializedContextsDto.builder()
            .serializedContexts(serializedContexts)
            .sqlType(sqlType)
            .shortContextLength(shortContextLength)
            .clobTypeToUse(getClobTypeToUse())
            .build());
  }

  private String serializeContext(ExecutionContext ctx) {
    Map<String, Object> m = new HashMap<>();
    for (Entry<String, Object> me : ctx.entrySet()) {
      m.put(me.getKey(), me.getValue());
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    String results;

    try {
      serializer.serialize(m, out);
      results = out.toString(StandardCharsets.ISO_8859_1);
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Could not serialize the execution context", ioe);
    }

    return results;
  }

  private ExecutionContext deserializeContext(String serializedCtx) {
    ExecutionContext executionContext = new ExecutionContext();
    Map<String, Object> map;
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(
          serializedCtx.getBytes(StandardCharsets.ISO_8859_1));
      map = serializer.deserialize(in);
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Unable to deserialize the execution context", ioe);
    }
    for (Entry<String, Object> entry : map.entrySet()) {
      executionContext.put(entry.getKey(), entry.getValue());
    }
    return executionContext;
  }
}
