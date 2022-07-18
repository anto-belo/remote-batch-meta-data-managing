package com.restdatabase.databaseservice.controller;

import com.restdatabase.databaseservice.dto.holder.JobExecutionHolder;
import com.restdatabase.databaseservice.dto.holder.JobExecutionParamHolder;
import com.restdatabase.databaseservice.service.JobExecutionService;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobExecution;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/dao/jobExec")
public class JobExecutionController {

  private final JobExecutionService dao;

  @GetMapping("/findJobExecutions")
  public List<JobExecution> findJobExecutions(@RequestParam Long jobInstanceId) {
    return dao.findJobExecutions(jobInstanceId);
  }

  @PostMapping("/saveJobExecution")
  public Long saveJobExecution(@RequestBody JobExecutionHolder holder) {
    return dao.saveJobExecution(holder);
  }

  @GetMapping("/countJobExecutions")
  public Integer countJobExecutions(@RequestParam Long jobExecutionId) {
    return dao.countJobExecutions(jobExecutionId);
  }

  @PostMapping("/updateJobExecution")
  public Integer updateJobExecution(@RequestBody JobExecutionHolder holder) {
    return dao.updateJobExecution(holder);
  }

  @GetMapping("/getJobExecutionVersion")
  public Integer getJobExecutionVersion(@RequestParam Long jobExecutionId) {
    return dao.getJobExecutionVersion(jobExecutionId);
  }

  @GetMapping("/getJobExecutionStatus")
  public String getJobExecutionStatus(@RequestParam Long jobExecutionId) {
    return dao.getJobExecutionStatus(jobExecutionId);
  }

  @GetMapping("/getLastJobExecution")
  public List<JobExecution> getLastJobExecution(@RequestParam Long jobInstanceId) {
    return dao.getLastJobExecution(jobInstanceId);
  }

  @GetMapping("/getJobExecution")
  public JobExecution getJobExecution(@RequestParam Long executionId) {
    return dao.getJobExecution(executionId);
  }

  @GetMapping("/findRunningJobExecutions")
  public Set<JobExecution> findRunningJobExecutions(@RequestParam String jobName) {
    return dao.findRunningJobExecutions(jobName);
  }

  @PostMapping("/createJobParameters")
  public void createJobParameters(@RequestBody JobExecutionParamHolder holder) {
    dao.createJobParameters(holder);
  }
}
