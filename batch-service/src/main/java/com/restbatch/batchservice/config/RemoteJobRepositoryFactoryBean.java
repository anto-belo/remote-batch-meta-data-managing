package com.restbatch.batchservice.config;

import static org.springframework.batch.support.DatabaseType.SYBASE;

import com.restbatch.batchservice.client.ExecutionContextDaoClient;
import com.restbatch.batchservice.client.JobExecutionDaoClient;
import com.restbatch.batchservice.client.JobInstanceDaoClient;
import com.restbatch.batchservice.client.StepExecutionDaoClient;
import com.restbatch.batchservice.dao.RemoteExecutionContextDao;
import com.restbatch.batchservice.dao.RemoteJobExecutionDao;
import com.restbatch.batchservice.dao.RemoteJobInstanceDao;
import com.restbatch.batchservice.dao.RemoteStepExecutionDao;
import java.lang.reflect.Field;
import java.sql.Types;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.batch.core.repository.support.AbstractJobRepositoryFactoryBean;
import org.springframework.batch.item.database.support.DefaultDataFieldMaxValueIncrementerFactory;
import org.springframework.batch.support.DatabaseType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Slf4j
@Setter
public class RemoteJobRepositoryFactoryBean extends AbstractJobRepositoryFactoryBean implements
    InitializingBean {

  private ExecutionContextDaoClient executionContextDaoClient;
  private JobInstanceDaoClient jobInstanceDaoClient;
  private JobExecutionDaoClient jobExecutionDaoClient;
  private StepExecutionDaoClient stepExecutionDaoClient;
  @Value("${database.type}")
  private String databaseType;
  private String tablePrefix = AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX;
  private int maxVarCharLength = AbstractJdbcBatchMetadataDao.DEFAULT_EXIT_MESSAGE_LENGTH;
  private ExecutionContextSerializer serializer;
  private Integer lobType;

  /**
   * @param type a value from the {@link Types} class to indicate the type to use for a CLOB
   */
  public void setClobType(int type) {
    this.lobType = type;
  }

  /**
   * A custom implementation of the {@link ExecutionContextSerializer}. The default, if not
   * injected, is the {@link Jackson2ExecutionContextStringSerializer}.
   *
   * @param serializer used to serialize/deserialize
   *                   {@link org.springframework.batch.item.ExecutionContext}
   * @see ExecutionContextSerializer
   */
  public void setSerializer(ExecutionContextSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Public setter for the length of long string columns in database. Do not set this if you haven't
   * modified the schema. Note this value will be used for the exit message in both
   * {@link RemoteJobExecutionDao} and {@link RemoteStepExecutionDao} and also the short version of
   * the execution context in {@link RemoteExecutionContextDao} . For databases with multibyte
   * character sets this number can be smaller (by up to a factor of 2 for 2-byte characters) than
   * the declaration of the column length in the DDL for the tables.
   *
   * @param maxVarCharLength the exitMessageLength to set
   */
  public void setMaxVarCharLength(int maxVarCharLength) {
    this.maxVarCharLength = maxVarCharLength;
  }

  /**
   * Sets the database type.
   *
   * @param dbType as specified by {@link DefaultDataFieldMaxValueIncrementerFactory}
   */
  public void setDatabaseType(String dbType) {
    databaseType = dbType;
  }

  /**
   * Sets the table prefix for all the batch meta-data tables.
   *
   * @param tablePrefix prefix prepended to batch meta-data tables
   */
  public void setTablePrefix(String tablePrefix) {
    this.tablePrefix = tablePrefix;
  }

  @Override
  public void afterPropertiesSet() throws Exception {

    Assert.notNull(executionContextDaoClient, "ExecutionContextDaoClient must not be null.");
    Assert.notNull(jobInstanceDaoClient, "JobInstanceDaoClient must not be null.");
    Assert.notNull(jobExecutionDaoClient, "JobExecutionDaoClient must not be null.");
    Assert.notNull(stepExecutionDaoClient, "StepExecutionDaoClient must not be null.");

    if (serializer == null) {
      serializer = new Jackson2ExecutionContextStringSerializer();
    }

    if (lobType != null) {
      Assert.isTrue(isValidTypes(lobType), "lobType must be a value from the java.sql.Types class");
    }

    super.afterPropertiesSet();
  }

  @Override
  protected JobInstanceDao createJobInstanceDao() {
    RemoteJobInstanceDao dao = new RemoteJobInstanceDao();
    dao.setClient(jobInstanceDaoClient);
    dao.setTablePrefix(tablePrefix);
    dao.afterPropertiesSet();
    return dao;
  }

  @Override
  protected JobExecutionDao createJobExecutionDao() {
    RemoteJobExecutionDao dao = new RemoteJobExecutionDao();
    dao.setClient(jobExecutionDaoClient);
    dao.setTablePrefix(tablePrefix);
    dao.setClobTypeToUse(determineClobTypeToUse(databaseType));
    dao.setExitMessageLength(maxVarCharLength);
    dao.afterPropertiesSet();
    return dao;
  }

  @Override
  protected StepExecutionDao createStepExecutionDao() {
    RemoteStepExecutionDao dao = new RemoteStepExecutionDao();
    dao.setClient(stepExecutionDaoClient);
    dao.setTablePrefix(tablePrefix);
    dao.setClobTypeToUse(determineClobTypeToUse(databaseType));
    dao.setExitMessageLength(maxVarCharLength);
    dao.afterPropertiesSet();
    return dao;
  }

  @Override
  protected ExecutionContextDao createExecutionContextDao() {
    RemoteExecutionContextDao dao = new RemoteExecutionContextDao();
    dao.setClient(executionContextDaoClient);
    dao.setTablePrefix(tablePrefix);
    dao.setClobTypeToUse(determineClobTypeToUse(databaseType));
    dao.setSerializer(serializer);

    dao.afterPropertiesSet();
    // Assume the same length.
    dao.setShortContextLength(maxVarCharLength);
    return dao;
  }

  private int determineClobTypeToUse(String databaseType) {
    if (lobType != null) {
      return lobType;
    } else {
      if (SYBASE == DatabaseType.valueOf(databaseType.toUpperCase())) {
        return Types.LONGVARCHAR;
      } else {
        return Types.CLOB;
      }
    }
  }

  private boolean isValidTypes(int value) throws Exception {
    boolean result = false;

    for (Field field : Types.class.getFields()) {
      int curValue = field.getInt(null);
      if (curValue == value) {
        result = true;
        break;
      }
    }

    return result;
  }
}
