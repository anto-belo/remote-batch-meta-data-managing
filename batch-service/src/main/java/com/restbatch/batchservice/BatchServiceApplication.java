package com.restbatch.batchservice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.restbatch.batchservice.deserializer.JobExecutionDeserializer;
import com.restbatch.batchservice.deserializer.JobInstanceDeserializer;
import feign.Logger;
import feign.Logger.Level;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;

@EnableFeignClients
@EnableBatchProcessing
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class BatchServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(BatchServiceApplication.class, args);
  }

  @Bean
  public Module jacksonConfig() {
    SimpleModule module = new SimpleModule();
    module.addDeserializer(JobInstance.class, new JobInstanceDeserializer());
    module.addDeserializer(JobExecution.class, new JobExecutionDeserializer());
    return module;
  }

  @Bean
  Logger.Level feignLoggerLevel() {
    return Level.BASIC;
  }
}
