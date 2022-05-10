package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static TaskController taskController;
    private static String site = "http://www.procontent.ru/"; // используемый сайт


    public static void main(String[] args) {
        taskController = new TaskController(site);
        Document doc = taskController.getUrl(site);
        String title;
        if (doc != null) {
            title = doc.title();
            log.info(title);
            taskController.ParseNews(doc,site);
        }
        return;
    }
}
