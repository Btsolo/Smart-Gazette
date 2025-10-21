package com.smartgazette.smartgazette;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync

public class SmartGazetteApplication {

    public static void main(String[] args) {
        SpringApplication.run(SmartGazetteApplication.class, args);
    }

}
