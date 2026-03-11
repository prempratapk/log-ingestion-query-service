package com.prempratapk.logingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LogIngestionApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogIngestionApplication.class, args);
    }
}
