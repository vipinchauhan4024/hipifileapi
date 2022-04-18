package com.volvo.hipi.hipifileapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;


@SpringBootApplication
@ComponentScan("com.volvo")
public class HipifileapiApplication {

	public static void main(String[] args) {
		SpringApplication.run(HipifileapiApplication.class, args);
	}

}
