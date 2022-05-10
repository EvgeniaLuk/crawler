package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

import java.io.IOException;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static TaskController taskController;
    public static String site = "http://www.procontent.ru/"; // используемый сайт


    public static void main(String[] args) throws InterruptedException {
        taskController = new TaskController(site);
        // Create producer thread
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run()
            {
                try {
                    taskController.produce();
                }

                catch (InterruptedException e) { // | IOException | TimeoutException добавить
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
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        // Start both threads
        t1.start();
        t2.start();

        // t1 finishes before t2
        t1.join();
        t2.join();






        return;
    }
}
