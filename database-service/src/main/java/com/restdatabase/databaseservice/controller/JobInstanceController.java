package com.restdatabase.databaseservice.controller;

import com.restdatabase.databaseservice.service.JobInstanceService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobInstance;
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
@RequestMapping("/dao/jobInst")
public class JobInstanceController {

  private final JobInstanceService dao;

  @PostMapping("/createJobInstance")
  public Long createJobInstance(@RequestBody Object[] args) {
    return dao.createJobInstance(args);
  }

  @GetMapping("/getJobInstance")
  public List<JobInstance> getJobInstance(@RequestParam String jobName,
      @RequestParam String jobKey) {
    return dao.getJobInstance(jobName, jobKey);
  }

  @GetMapping("/getJobInstanceById")
  public JobInstance getJobInstance(@RequestParam(required = false) Long instanceId) {
    return dao.getJobInstance(instanceId);
  }

  @GetMapping("/getJobNames")
  public List<String> getJobNames() {
    return dao.getJobNames();
  }

  @GetMapping("/getJobInstances")
  public List<JobInstance> getJobInstances(@RequestParam String jobName, @RequestParam int start,
      @RequestParam int count) {
    return dao.getJobInstances(jobName, start, count);
  }

  @GetMapping("/getLastJobInstance")
  public JobInstance getLastJobInstance(@RequestParam String jobName) {
    return dao.getLastJobInstance(jobName);
  }

  @GetMapping("/getJobInstanceByJobExecutionId")
  public JobInstance getJobInstanceByJobExecutionId(@RequestParam Long jobExecutionId) {
    return dao.getJobInstanceByJobExecutionId(jobExecutionId);
  }

  @GetMapping("/getJobInstanceCount")
  public Object getJobInstanceCount(@RequestParam(required = false) String jobName) {
    return dao.getJobInstanceCount(jobName);
  }

  @GetMapping("/findJobInstancesByName")
  public List<JobInstance> findJobInstancesByName(@RequestParam String jobName,
      @RequestParam int start, @RequestParam int count) {
    return dao.findJobInstancesByName(jobName, start, count);
  }
}
