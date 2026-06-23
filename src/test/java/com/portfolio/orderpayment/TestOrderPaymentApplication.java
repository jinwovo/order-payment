package com.portfolio.orderpayment;

import org.springframework.boot.SpringApplication;

public class TestOrderPaymentApplication {

	public static void main(String[] args) {
		SpringApplication.from(OrderPaymentApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
