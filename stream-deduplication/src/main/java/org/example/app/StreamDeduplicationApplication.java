package org.example.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class StreamDeduplicationApplication {
    public static void main(String[] args) {
        SpringApplication.run(StreamDeduplicationApplication.class, args);
    }
}
