package com.restbatch.batchservice.job;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Data
@AllArgsConstructor
public class Post {

  private Integer id;
  private String title;
}
