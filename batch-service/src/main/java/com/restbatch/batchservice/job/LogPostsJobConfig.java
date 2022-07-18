package com.restbatch.batchservice.job;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Configuration
@RequiredArgsConstructor
public class LogPostsJobConfig {

  private final JobBuilderFactory jobs;
  private final StepBuilderFactory steps;

  @Bean
  public Job sendJob(Step logging) {
    return jobs.get("logPostsJob")
        .start(logging)
        .build();
  }

  @Bean
  protected Step logging(ItemReader<Post> reader,
      ItemWriter<Post> writer) {
    return steps.get("logging")
        .<Post, Post>chunk(3)
        .reader(reader)
        .writer(writer)
        .allowStartIfComplete(true)
        .build();
  }
}
