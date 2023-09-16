package com.bw.kafka02;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class Driver implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(Driver.class, args);
    }

    /*
    * java -jar xx.jar com.bw.kafka02.UserConsumer users*/
    @Override
    public void run(String... args) throws Exception {
        if (args == null && args.length < 1) {
            throw new Exception("参数错误");
        }

        Object o = Class.forName(args[0]).newInstance();
        if (o instanceof ExecutorProgram) {
            ((ExecutorProgram) o).executor(Arrays.copyOfRange(args, 1, args.length));
        } else {
            throw new Exception("不是指定类型，无法执行");
        }

    }

}
