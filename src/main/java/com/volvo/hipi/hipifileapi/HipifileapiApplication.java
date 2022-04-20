package com.volvo.hipi.hipifileapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan("com.volvo")
public class HipifileapiApplication {

	public static void main(String[] args) {
		SpringApplication.run(HipifileapiApplication.class, args);
	}

}
