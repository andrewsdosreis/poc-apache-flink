package com.example.pocflink;

import com.example.pocflink.service.FirstExample;

public class FlinkApplication {

	public static void main(String[] args) throws Exception {

		FirstExample firstExample = new FirstExample();
		firstExample.run(args);
	}
}