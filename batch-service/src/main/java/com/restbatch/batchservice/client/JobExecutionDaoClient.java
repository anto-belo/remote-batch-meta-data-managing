package com.restbatch.batchservice.client;

import com.restbatch.batchservice.dto.holder.JobExecutionHolder;
import com.restbatch.batchservice.dto.holder.JobExecutionParamHolder;
import java.util.List;
import java.util.Set;
import org.springframework.batch.core.JobExecution;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@FeignClient(name = "jobExecDaoClient", url = "${api.base-url}/dao/jobExec")
public interface JobExecutionDaoClient {

  @GetMapping("/findJobExecutions")
  List<JobExecution> findJobExecutions(@RequestParam Long jobInstanceId);

  @PostMapping("/saveJobExecution")
  Long saveJobExecution(@RequestBody JobExecutionHolder holder);

  @GetMapping("/countJobExecutions")
  Integer countJobExecutions(@RequestParam Long jobExecutionId);

  @PostMapping("/updateJobExecution")
  Integer updateJobExecution(@RequestBody JobExecutionHolder holder);

  @GetMapping("/getJobExecutionVersion")
  Integer getJobExecutionVersion(@RequestParam Long jobExecutionId);

  @GetMapping("/getJobExecutionStatus")
  String getJobExecutionStatus(@RequestParam Long jobExecutionId);

  @GetMapping("/getLastJobExecution")
  List<JobExecution> getLastJobExecution(@RequestParam Long jobInstanceId);

  @GetMapping("/getJobExecution")
  JobExecution getJobExecution(@RequestParam Long executionId);

  @GetMapping("/findRunningJobExecutions")
  Set<JobExecution> findRunningJobExecutions(@RequestParam String jobName);

  @PostMapping("/createJobParameters")
  void createJobParameters(@RequestBody JobExecutionParamHolder holder);
}
