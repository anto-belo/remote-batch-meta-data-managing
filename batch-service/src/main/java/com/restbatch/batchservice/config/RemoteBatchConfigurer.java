package com.restbatch.batchservice.config;

import com.restbatch.batchservice.client.ExecutionContextDaoClient;
import com.restbatch.batchservice.client.JobExecutionDaoClient;
import com.restbatch.batchservice.client.JobInstanceDaoClient;
import com.restbatch.batchservice.client.StepExecutionDaoClient;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RemoteBatchConfigurer implements BatchConfigurer {

  private final ExecutionContextDaoClient executionContextDaoClient;
  private final JobInstanceDaoClient jobInstanceDaoClient;
  private final JobExecutionDaoClient jobExecutionDaoClient;
  private final StepExecutionDaoClient stepExecutionDaoClient;

  @Value("${database.type}")
  private String databaseType;

  private JobRepository jobRepository;
  private JobLauncher jobLauncher;
  private JobExplorer jobExplorer;

  @Override
  public JobRepository getJobRepository() {
    return jobRepository;
  }

  @Override
  public PlatformTransactionManager getTransactionManager() {
    return new ResourcelessTransactionManager();
  }

  @Override
  public JobLauncher getJobLauncher() {
    return jobLauncher;
  }

  @Override
  public JobExplorer getJobExplorer() {
    return jobExplorer;
  }

  @PostConstruct
  public void initialize() {
    try {
      jobRepository = createJobRepository();
      jobExplorer = createJobExplorer();
      jobLauncher = createJobLauncher();
    } catch (Exception e) {
      throw new BatchConfigurationException(e);
    }
  }

  protected JobLauncher createJobLauncher() throws Exception {
    SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
    jobLauncher.setJobRepository(jobRepository);
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }

  protected JobRepository createJobRepository() throws Exception {
    RemoteJobRepositoryFactoryBean factory = new RemoteJobRepositoryFactoryBean();
    factory.setExecutionContextDaoClient(executionContextDaoClient);
    factory.setJobInstanceDaoClient(jobInstanceDaoClient);
    factory.setJobExecutionDaoClient(jobExecutionDaoClient);
    factory.setStepExecutionDaoClient(stepExecutionDaoClient);
    factory.setTransactionManager(getTransactionManager());
    factory.setDatabaseType(databaseType);
    factory.afterPropertiesSet();
    return factory.getObject();
  }

  protected JobExplorer createJobExplorer() {
    RemoteJobExplorerFactoryBean factory = new RemoteJobExplorerFactoryBean();
    factory.setExecutionContextDaoClient(executionContextDaoClient);
    factory.setJobInstanceDaoClient(jobInstanceDaoClient);
    factory.setJobExecutionDaoClient(jobExecutionDaoClient);
    factory.setStepExecutionDaoClient(stepExecutionDaoClient);
    factory.afterPropertiesSet();
    return factory.getObject();
  }
}
