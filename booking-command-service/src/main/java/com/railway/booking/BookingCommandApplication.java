package com.railway.booking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableFeignClients
@EnableScheduling
public class BookingCommandApplication {
    public static void main(String[] args) {
        SpringApplication.run(BookingCommandApplication.class, args);
    }
}
