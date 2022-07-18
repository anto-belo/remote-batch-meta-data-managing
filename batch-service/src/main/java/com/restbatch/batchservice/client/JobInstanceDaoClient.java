package com.restbatch.batchservice.client;

import java.util.List;
import org.springframework.batch.core.JobInstance;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@FeignClient(name = "jobInstDaoClient", url = "${api.base-url}/dao/jobInst")
public interface JobInstanceDaoClient {

  @PostMapping("/createJobInstance")
  Long createJobInstance(@RequestBody Object[] args);

  @GetMapping("/getJobInstance")
  List<JobInstance> getJobInstance(@RequestParam String jobName,
      @RequestParam String jobKey);

  @GetMapping("/getJobInstanceById")
  JobInstance getJobInstance(@RequestParam(required = false) Long instanceId);

  @GetMapping("/getJobNames")
  List<String> getJobNames();

  @GetMapping("/getJobInstances")
  List<JobInstance> getJobInstances(@RequestParam String jobName, @RequestParam int start,
      @RequestParam int count);

  @GetMapping("/getLastJobInstance")
  JobInstance getLastJobInstance(@RequestParam String jobName);

  @GetMapping("/getJobInstanceByJobExecutionId")
  JobInstance getJobInstanceByJobExecutionId(@RequestParam Long jobExecutionId);

  @GetMapping("/getJobInstanceCount")
  Object getJobInstanceCount(@RequestParam(required = false) String jobName);

  @GetMapping("/findJobInstancesByName")
  List<JobInstance> findJobInstancesByName(@RequestParam String jobName,
      @RequestParam int start, @RequestParam int count);
}
