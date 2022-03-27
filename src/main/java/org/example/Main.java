package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Main {
    private static Logger log = LogManager.getLogger();
    private static TaskController taskController;
    private static String site = "http://www.procontent.ru/";

    static public void ParseNews(Document doc) {
        Elements news = doc.getElementsByClass("homepage-news");
        for (Element element: news) {
            try {
                Element etitle = element.child(0).child(0);
                String link = site + etitle.attr("href");
                String text = taskController.GetPage(link, etitle.text());
                log.info(text);
            } catch (Exception e) {
                log.error(e);
            }
        }
        return ;
    }
    public static void main(String[] args) {
        taskController = new TaskController(site);
        Document doc = taskController.getUrl(site);
        String title;
        if (doc != null) {
            title = doc.title();
            log.info(title);
            ParseNews(doc);
        }
        return;
    }
}
