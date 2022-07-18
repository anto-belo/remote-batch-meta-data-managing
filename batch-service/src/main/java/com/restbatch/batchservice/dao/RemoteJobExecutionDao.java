package com.restbatch.batchservice.dao;

import com.restbatch.batchservice.client.JobExecutionDaoClient;
import com.restbatch.batchservice.dto.holder.JobExecutionHolder;
import com.restbatch.batchservice.dto.holder.JobExecutionParamHolder;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameter.ParameterType;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Slf4j
public class RemoteJobExecutionDao extends AbstractBatchMetaDataDao
    implements JobExecutionDao, InitializingBean {

  @Setter
  private JobExecutionDaoClient client;
  private int exitMessageLength = DEFAULT_EXIT_MESSAGE_LENGTH;

  /**
   * Public setter for the exit message length in database. Do not set this if you haven't modified
   * the schema.
   *
   * @param exitMessageLength the exitMessageLength to set
   */
  public void setExitMessageLength(int exitMessageLength) {
    this.exitMessageLength = exitMessageLength;
  }

  @Override
  public void afterPropertiesSet() {
    Assert.notNull(client, "JobExecutionDaoClient must not be null.");
  }

  @Override
  public List<JobExecution> findJobExecutions(final JobInstance job) {

    Assert.notNull(job, "Job cannot be null.");
    Assert.notNull(job.getId(), "Job Id cannot be null.");

    return client.findJobExecutions(job.getInstanceId()).stream()
        .peek(execution -> execution.setJobInstance(job))
        .collect(Collectors.toList());
  }

  /**
   * SQL implementation using Sequences via the Spring incrementer abstraction. Once a new id has
   * been obtained, the JobExecution is saved via a SQL INSERT statement.
   *
   * @throws IllegalArgumentException if jobExecution is null, as well as any of it's fields to be
   *                                  persisted.
   * @see org.springframework.batch.core.repository.dao.JobExecutionDao#saveJobExecution(JobExecution)
   */
  @Override
  public void saveJobExecution(JobExecution jobExecution) {

    validateJobExecution(jobExecution);

    jobExecution.incrementVersion();

    JobExecutionHolder holder = JobExecutionHolder.builder()
        .jobId(jobExecution.getJobId())
        .startTime(jobExecution.getStartTime())
        .endTime(jobExecution.getEndTime())
        .status(jobExecution.getStatus().toString())
        .exitCode(jobExecution.getExitStatus().getExitCode())
        .exitDescription(jobExecution.getExitStatus().getExitDescription())
        .version(jobExecution.getVersion())
        .createTime(jobExecution.getCreateTime())
        .lastUpdated(jobExecution.getLastUpdated())
        .jobConfigurationName(jobExecution.getJobConfigurationName())
        .build();

    Long jobExecId = client.saveJobExecution(holder);

    jobExecution.setId(jobExecId);
    insertJobParameters(jobExecId, jobExecution.getJobParameters());
  }

  /**
   * Validate JobExecution. At a minimum, JobId, Status, CreateTime cannot be null.
   */
  private void validateJobExecution(JobExecution jobExecution) {

    Assert.notNull(jobExecution, "jobExecution cannot be null");
    Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
    Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
    Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
  }

  /**
   * Update given JobExecution using a SQL UPDATE statement. The JobExecution is first checked to
   * ensure all fields are not null, and that it has an ID. The database is then queried to ensure
   * that the ID exists, which ensures that it is valid.
   *
   * @see org.springframework.batch.core.repository.dao.JobExecutionDao#updateJobExecution(JobExecution)
   */
  @Override
  public void updateJobExecution(JobExecution jobExecution) {

    validateJobExecution(jobExecution);

    Assert.notNull(jobExecution.getId(),
        "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");

    Assert.notNull(jobExecution.getVersion(),
        "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

    synchronized (jobExecution) {
      int version = jobExecution.getVersion() + 1;

      String exitDescription = jobExecution.getExitStatus().getExitDescription();
      if (exitDescription != null && exitDescription.length() > exitMessageLength) {
        exitDescription = exitDescription.substring(0, exitMessageLength);
        if (log.isDebugEnabled()) {
          log.debug("Truncating long message before update of JobExecution: " + jobExecution);
        }
      }

      // Check if given JobExecution's id already exists, if none is found
      // it is invalid and an exception should be thrown.
      if (client.countJobExecutions(jobExecution.getId()) != 1) {
        throw new NoSuchObjectException(
            "Invalid JobExecution, ID " + jobExecution.getId() + " not found.");
      }

      JobExecutionHolder holder = JobExecutionHolder.builder()
          .startTime(jobExecution.getStartTime())
          .endTime(jobExecution.getEndTime())
          .status(jobExecution.getStatus().toString())
          .exitCode(jobExecution.getExitStatus().getExitCode())
          .exitDescription(exitDescription)
          .updateVersion(version)
          .createTime(jobExecution.getCreateTime())
          .lastUpdated(jobExecution.getLastUpdated())
          .id(jobExecution.getId())
          .version(jobExecution.getVersion())
          .build();

      int count = client.updateJobExecution(holder);

      // Avoid concurrent modifications...
      if (count == 0) {
        int currentVersion = client.getJobExecutionVersion(jobExecution.getId());
        throw new OptimisticLockingFailureException("Attempt to update job execution id="
            + jobExecution.getId() + " with wrong version (" + jobExecution.getVersion()
            + "), where current version is " + currentVersion);
      }

      jobExecution.incrementVersion();
    }
  }

  @Nullable
  @Override
  public JobExecution getLastJobExecution(JobInstance jobInstance) {

    List<JobExecution> executions = client.getLastJobExecution(jobInstance.getInstanceId());

    Assert.state(executions.size() <= 1, "There must be at most one latest job execution");

    if (executions.isEmpty()) {
      return null;
    } else {
      JobExecution jobExecution = executions.get(0);
      jobExecution.setJobInstance(jobInstance);
      return jobExecution;
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.springframework.batch.core.repository.dao.JobExecutionDao#
   * getLastJobExecution(java.lang.String)
   */
  @Override
  @Nullable
  public JobExecution getJobExecution(Long executionId) {
    return client.getJobExecution(executionId);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.springframework.batch.core.repository.dao.JobExecutionDao#
   * findRunningJobExecutions(java.lang.String)
   */
  @Override
  public Set<JobExecution> findRunningJobExecutions(String jobName) {
    return client.findRunningJobExecutions(jobName);
  }

  @Override
  public void synchronizeStatus(JobExecution jobExecution) {
    int currentVersion = client.getJobExecutionVersion(jobExecution.getId());

    if (currentVersion != jobExecution.getVersion()) {
      String status = client.getJobExecutionStatus(jobExecution.getId());
      jobExecution.upgradeStatus(BatchStatus.valueOf(status));
      jobExecution.setVersion(currentVersion);
    }
  }

  /**
   * Convenience method that inserts all parameters from the provided JobParameters.
   */
  private void insertJobParameters(Long executionId, JobParameters jobParameters) {

    for (Entry<String, JobParameter> entry : jobParameters.getParameters()
        .entrySet()) {
      JobParameter jobParameter = entry.getValue();
      insertParameter(executionId, jobParameter.getType(), entry.getKey(),
          jobParameter.getValue(), jobParameter.isIdentifying());
    }
  }

  /**
   * Convenience method that inserts an individual records into the JobParameters table.
   */
  private void insertParameter(Long executionId, ParameterType type, String key,
      Object value, boolean identifying) {

    String identifyingFlag = identifying ? "Y" : "N";

    JobExecutionParamHolder.JobExecutionParamHolderBuilder builder =
        JobExecutionParamHolder.builder()
            .jobExecutionId(executionId)
            .keyName(key)
            .type(type.toString())
            .identifyingFlag(identifyingFlag);

    if (type == ParameterType.STRING) {
      builder.stringValue((String) value);
    } else if (type == ParameterType.LONG) {
      builder.longValue((Long) value);
    } else if (type == ParameterType.DOUBLE) {
      builder.doubleValue((Double) value);
    } else if (type == ParameterType.DATE) {
      builder.dateValue((Timestamp) value);
    }

    client.createJobParameters(builder.build());
  }
}
