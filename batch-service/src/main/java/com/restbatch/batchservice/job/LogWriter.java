package com.restbatch.batchservice.job;

import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LogWriter implements ItemWriter<Post> {

  @Override
  public void write(List<? extends Post> list) {
    list.forEach(p -> log.info(p.toString()));
    log.info("Chunks amount: {}", list.size());
  }
}
