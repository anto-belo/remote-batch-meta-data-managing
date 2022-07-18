package com.restbatch.batchservice.config;

import com.restbatch.batchservice.client.ExecutionContextDaoClient;
import com.restbatch.batchservice.client.JobExecutionDaoClient;
import com.restbatch.batchservice.client.JobInstanceDaoClient;
import com.restbatch.batchservice.client.StepExecutionDaoClient;
import com.restbatch.batchservice.dao.RemoteExecutionContextDao;
import com.restbatch.batchservice.dao.RemoteJobExecutionDao;
import com.restbatch.batchservice.dao.RemoteJobInstanceDao;
import com.restbatch.batchservice.dao.RemoteStepExecutionDao;
import lombok.Setter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.AbstractJobExplorerFactoryBean;
import org.springframework.batch.core.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.dao.AbstractJdbcBatchMetadataDao;
import org.springframework.batch.core.repository.dao.ExecutionContextDao;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Setter
public class RemoteJobExplorerFactoryBean extends AbstractJobExplorerFactoryBean
    implements InitializingBean {

  private ExecutionContextDaoClient executionContextDaoClient;
  private JobInstanceDaoClient jobInstanceDaoClient;
  private JobExecutionDaoClient jobExecutionDaoClient;
  private StepExecutionDaoClient stepExecutionDaoClient;
  private String tablePrefix = AbstractJdbcBatchMetadataDao.DEFAULT_TABLE_PREFIX;
  private ExecutionContextSerializer serializer;

  /**
   * A custom implementation of the {@link ExecutionContextSerializer}. The default, if not
   * injected, is the {@link Jackson2ExecutionContextStringSerializer}.
   *
   * @param serializer used to serialize/deserialize an
   *                   {@link org.springframework.batch.item.ExecutionContext}
   * @see ExecutionContextSerializer
   */
  public void setSerializer(ExecutionContextSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Sets the table prefix for all the batch meta-data tables.
   *
   * @param tablePrefix prefix for the batch meta-data tables
   */
  public void setTablePrefix(String tablePrefix) {
    this.tablePrefix = tablePrefix;
  }

  @Override
  public void afterPropertiesSet() {
    Assert.notNull(executionContextDaoClient, "ExecutionContextDaoClient must not be null.");
    Assert.notNull(jobInstanceDaoClient, "JobInstanceDaoClient must not be null.");
    Assert.notNull(jobExecutionDaoClient, "JobExecutionDaoClient must not be null.");
    Assert.notNull(stepExecutionDaoClient, "StepExecutionDaoClient must not be null.");

    if (serializer == null) {
      serializer = new Jackson2ExecutionContextStringSerializer();
    }
  }

  private JobExplorer getTarget() {
    return new SimpleJobExplorer(createJobInstanceDao(),
        createJobExecutionDao(), createStepExecutionDao(),
        createExecutionContextDao());
  }

  @Override
  protected ExecutionContextDao createExecutionContextDao() {
    RemoteExecutionContextDao dao = new RemoteExecutionContextDao();
    dao.setClient(executionContextDaoClient);
    dao.setTablePrefix(tablePrefix);
    dao.setSerializer(serializer);
    dao.afterPropertiesSet();
    return dao;
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
    dao.afterPropertiesSet();
    return dao;
  }

  @Override
  protected StepExecutionDao createStepExecutionDao() {
    RemoteStepExecutionDao dao = new RemoteStepExecutionDao();
    dao.setClient(stepExecutionDaoClient);
    dao.setTablePrefix(tablePrefix);
    dao.afterPropertiesSet();
    return dao;
  }

  @Override
  public JobExplorer getObject() {
    return getTarget();
  }
}
