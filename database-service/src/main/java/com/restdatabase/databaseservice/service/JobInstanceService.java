package com.restdatabase.databaseservice.service;

import java.util.List;
import org.springframework.batch.core.JobInstance;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
public interface JobInstanceService {

  Long createJobInstance(Object[] args);

  List<JobInstance> getJobInstance(String jobName, String jobKey);

  JobInstance getJobInstance(Long instanceId);

  List<String> getJobNames();

  List<JobInstance> getJobInstances(String jobName, int start, int count);

  JobInstance getLastJobInstance(String jobName);

  JobInstance getJobInstanceByJobExecutionId(Long jobExecutionId);

  Object getJobInstanceCount(String jobName);

  List<JobInstance> findJobInstancesByName(String jobName, int start, int count);
}
