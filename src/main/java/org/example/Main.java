package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static TaskController taskController;
    public static String site = "http://www.procontent.ru/"; // используемый сайт
    public static String QUEUE_NAME_1 = "queue_1";
    public static String QUEUE_NAME_2 = "queue_2";




    public static void main(String[] args) throws InterruptedException, IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setVirtualHost("/");
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");

        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME_1, false, false, false, null);
        channel.queueDeclare(QUEUE_NAME_2, false, false, false, null);
        channel.close();
        connection.close();


        taskController = new TaskController(site);
        // Create producer thread
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run()
            {
                try {
                    taskController.produce();
                }

                catch (InterruptedException | IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });


        // Create consumer thread
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    taskController.consume();
                }
                catch (InterruptedException | IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t3 = new Thread(new Runnable() {
            @Override
            public void run()
            {
                try {
                    taskController.send();
                }

                catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread t4 = new Thread(new Runnable() {
            @Override
            public void run()
            {
                try {
                    // Запрос в БД
                    taskController.request();
                }
                catch (UnknownHostException | ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // Start both threads
        t1.start();
        t2.start();
        t3.start();
        t4.start();

        // t1 finishes before t2
        t1.join();
        t2.join();
        t3.join();
        t4.join();

        return;
    }
}
