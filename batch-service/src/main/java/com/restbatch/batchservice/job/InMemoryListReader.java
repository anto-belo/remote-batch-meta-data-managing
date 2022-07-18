package com.restbatch.batchservice.job;

import java.util.ArrayList;
import java.util.List;
import org.springframework.batch.item.ItemReader;
import org.springframework.stereotype.Component;

/**
 * @author Anton Belousov
 * @since 0.0.1-SNAPSHOT
 */
@Component
public class InMemoryListReader implements ItemReader<Post> {

  private final List<Post> posts = new ArrayList<>(List.of(
      new Post(1, "A"),
      new Post(2, "B"),
      new Post(3, "C"),
      new Post(4, "D"),
      new Post(5, "E"),
      new Post(6, "F"),
      new Post(7, "G"),
      new Post(8, "H"),
      new Post(9, "I"),
      new Post(10, "J")
  ));

  @Override
  public Post read() {
    if (!posts.isEmpty()) {
      return posts.remove(0);
    }
    return null;
  }
}
