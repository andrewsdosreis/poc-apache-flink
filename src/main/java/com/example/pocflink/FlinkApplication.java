package com.example.pocflink;

import com.example.pocflink.service.Challenge;

public class FlinkApplication {

	public static void main(String[] args) throws Exception {

		Challenge challenge = new Challenge();
		challenge.run(args);
	}
}