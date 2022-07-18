package com.restbatch.batchservice.dao;

import com.restbatch.batchservice.client.JobInstanceDaoClient;
import java.util.List;
import lombok.Setter;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public class RemoteJobInstanceDao extends AbstractBatchMetaDataDao
    implements JobInstanceDao, InitializingBean {

  private final JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

  @Setter
  private JobInstanceDaoClient client;

  /**
   * In this JDBC implementation a job id is obtained by asking the jobIncrementer (which is likely
   * a sequence) for the next long value, and then passing the id and parameter values into an
   * INSERT statement.
   *
   * @throws IllegalArgumentException if any {@link JobParameters} fields are null.
   * @see org.springframework.batch.core.repository.dao.JobInstanceDao#createJobInstance(String,
   * JobParameters)
   */
  @Override
  public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {

    Assert.notNull(jobName, "Job name must not be null.");
    Assert.notNull(jobParameters, "JobParameters must not be null.");

    Assert.state(getJobInstance(jobName, jobParameters) == null,
        "JobInstance must not already exist");

    JobInstance jobInstance = new JobInstance(0L, jobName);
    jobInstance.incrementVersion();

    Object[] parameters = new Object[]{jobName, jobKeyGenerator.generateKey(jobParameters),
        jobInstance.getVersion()};
    Long jobId = client.createJobInstance(parameters);
    jobInstance.setId(jobId);

    return jobInstance;
  }

  /**
   * The job table is queried for <strong>any</strong> jobs that match the given identifier, adding
   * them to a list via the RowMapper callback.
   *
   * @throws IllegalArgumentException if any {@link JobParameters} fields are null.
   * @see org.springframework.batch.core.repository.dao.JobInstanceDao#getJobInstance(String,
   * JobParameters)
   */
  @Override
  @Nullable
  public JobInstance getJobInstance(final String jobName, final JobParameters jobParameters) {

    Assert.notNull(jobName, "Job name must not be null.");
    Assert.notNull(jobParameters, "JobParameters must not be null.");

    String jobKey = jobKeyGenerator.generateKey(jobParameters);

    List<JobInstance> instances = client.getJobInstance(jobName, jobKey);

    if (instances.isEmpty()) {
      return null;
    } else {
      Assert.state(instances.size() == 1, "instance count must be 1 but was " + instances.size());
      return instances.get(0);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.springframework.batch.core.repository.dao.JobInstanceDao#getJobInstance
   * (java.lang.Long)
   */
  @Override
  @Nullable
  public JobInstance getJobInstance(@Nullable Long instanceId) {
    return client.getJobInstance(instanceId);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.springframework.batch.core.repository.dao.JobInstanceDao#getJobNames
   * ()
   */
  @Override
  public List<String> getJobNames() {
    return client.getJobNames();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.springframework.batch.core.repository.dao.JobInstanceDao#
   * getLastJobInstances(java.lang.String, int)
   */
  @Override
  public List<JobInstance> getJobInstances(String jobName, final int start, final int count) {
    return client.getJobInstances(jobName, start, count);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.springframework.batch.core.repository.dao.JobInstanceDao#
   * getLastJobInstance(java.lang.String)
   */
  @Override
  @Nullable
  public JobInstance getLastJobInstance(String jobName) {
    return client.getLastJobInstance(jobName);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.springframework.batch.core.repository.dao.JobInstanceDao#getJobInstance
   * (org.springframework.batch.core.JobExecution)
   */
  @Override
  @Nullable
  public JobInstance getJobInstance(JobExecution jobExecution) {
    return client.getJobInstanceByJobExecutionId(jobExecution.getId());
  }

  /* (non-Javadoc)
   * @see org.springframework.batch.core.repository.dao.JobInstanceDao#getJobInstanceCount(java.lang.String)
   */
  @Override
  public int getJobInstanceCount(@Nullable String jobName) throws NoSuchJobException {
    Object result = client.getJobInstanceCount(jobName);
    if (result instanceof Integer) {
      return (Integer) result;
    }
    throw (NoSuchJobException) result;
  }

  @Override
  public void afterPropertiesSet() {
    Assert.notNull(client, "JobInstanceDaoClient must not be null.");
  }

  @Override
  public List<JobInstance> findJobInstancesByName(String jobName, final int start,
      final int count) {
    return client.findJobInstancesByName(jobName, start, count);
  }
}
