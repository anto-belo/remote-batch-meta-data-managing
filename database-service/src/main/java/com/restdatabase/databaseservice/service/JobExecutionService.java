package com.restdatabase.databaseservice.service;

import com.restdatabase.databaseservice.dto.holder.JobExecutionHolder;
import com.restdatabase.databaseservice.dto.holder.JobExecutionParamHolder;
import java.util.List;
import java.util.Set;
import org.springframework.batch.core.JobExecution;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public interface JobExecutionService {

  List<JobExecution> findJobExecutions(Long jobInstanceId);

  Long saveJobExecution(JobExecutionHolder holder);

  Integer countJobExecutions(Long jobExecutionId);

  Integer updateJobExecution(JobExecutionHolder holder);

  Integer getJobExecutionVersion(Long jobExecutionId);

  String getJobExecutionStatus(Long jobExecutionId);

  List<JobExecution> getLastJobExecution(Long jobInstanceId);

  JobExecution getJobExecution(Long executionId);

  Set<JobExecution> findRunningJobExecutions(String jobName);

  void createJobParameters(JobExecutionParamHolder holder);
}
