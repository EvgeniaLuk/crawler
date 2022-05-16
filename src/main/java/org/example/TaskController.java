package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

public class TaskController {
    private static Logger log = LogManager.getLogger();
    private CloseableHttpClient client = null;
    private HttpClientBuilder builder;
    private String server;
    private int retryDelay = 5 * 1000;
    private int retryCount = 2;
    private int metadataTimeout = 30 * 1000;

    ConnectionFactory settings;

    public TaskController(String _server) {
        CookieStore httpCookieStore = new BasicCookieStore();
        builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookieStore);
        client = builder.build();
        this.server = _server;


        settings = new ConnectionFactory();
        settings.setHost("127.0.0.1");
        settings.setPort(5672);
        settings.setVirtualHost("/");
        settings.setUsername("rabbitmq");
        settings.setPassword("rabbitmq");
    }

    public Document getUrl(String url) {
        //String url = server + "/news/" + newsId;
        int code = 0;
        boolean bStop = false;
        Document doc = null;
        for (int iTry = 0; iTry < retryCount && !bStop; iTry++) {
            log.info("getting page from url " + url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(metadataTimeout)
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url);
            request.setConfig(requestConfig);
            CloseableHttpResponse response = null;
            try {
                response = client.execute(request);
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    log.warn("error get url " + url + " code " + code);
                    bStop = true;//break;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try {
                            doc = Jsoup.parse(entity.getContent(), "windows-1251", server); // для данного сайта кодировка windows-1251
                            break;
                        } catch (IOException e) {
                            log.error(e);
                        }
                    }
                    bStop = true;//break;
                } else {
                    //if (code == 403) {
                    log.warn("error get url " + url + " code " + code);
                    response.close();
                    response = null;
                    client.close();
                    CookieStore httpCookieStore = new BasicCookieStore();
                    builder.setDefaultCookieStore(httpCookieStore);
                    client = builder.build();
                    int delay = retryDelay * 1000 * (iTry + 1);
                    log.info("wait " + delay / 1000 + " s...");
                    try {
                        Thread.sleep(delay);
                        continue;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } catch (IOException e) {
                log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
        return doc;
    }

    public String GetPage(String link) throws IOException, TimeoutException {
        Document ndoc = getUrl(link);
        String header = ndoc.child(0).child(0).text();
        String text = "";
        if (ndoc != null) {
            Connection connection = settings.newConnection();
            Channel channel = connection.createChannel();

            Elements newsDoc = ndoc.getElementsByClass("news_body");
            log.info("Текст: " + newsDoc.text());

            log.info("Заголовок: " + header);

            String time = ndoc.getElementsByClass("news_publish_date").text();
            log.info("Время публикация: " + time);

            String author = ndoc.select("span[itemprop='name']").text();
            log.info("Автор: " + author);

            log.info("Ссылка: " + link);

            log.info("\n");

            // Оправляем в очередь_2
            Json json = new Json(header, newsDoc.text(), author, link, time);
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json_complete = ow.writeValueAsString(json);
            channel.basicPublish("", Main.QUEUE_NAME_2, null, json_complete.getBytes());

            channel.close();
            connection.close();
        }

        return text;
    }

    public void ParseNews(Document doc, String site) throws InterruptedException, IOException, TimeoutException {
        Connection connection = settings.newConnection();
        Channel channel = connection.createChannel();

        Elements news = doc.getElementsByClass("homepage-news");
        for (Element element: news) {
            try {
                Element etitle = element.child(0).child(0);
                String link = site + etitle.attr("href");
                channel.basicPublish("", Main.QUEUE_NAME_1, null, link.getBytes());
            } catch (Exception e) {
                log.error(e);
            }
        }
        channel.close();
        connection.close();

        return;
    }

    void produce ()   throws InterruptedException, IOException, TimeoutException{

        log.info("Produce go");
        Document doc = getUrl(Main.site);
        String title;
        if (doc != null) {
            title = doc.title();
            log.info(title);
            ParseNews(doc, Main.site);
        }

    }
    void consume () throws InterruptedException, IOException, TimeoutException {
        Connection connection = settings.newConnection();
        Channel channel = connection.createChannel();


        log.info("Consume go");

        while (true) {
            synchronized (this) {
                try {
                    if (channel.messageCount(Main.QUEUE_NAME_1) == 0) continue;
                    String url = new String(channel.basicGet(Main.QUEUE_NAME_1, true).getBody(), StandardCharsets.UTF_8);
                    if (url!=null)
                        GetPage(url);
                    notify();
                }
                catch (IndexOutOfBoundsException e) {
                    wait();
                }
            }
        }
    }

    void send () throws IOException, TimeoutException {
        while (true){
            Connection connection = settings.newConnection();
            Channel channel = connection.createChannel();

            if (channel.messageCount(Main.QUEUE_NAME_2) == 0) continue;
            String json = new String(channel.basicGet(Main.QUEUE_NAME_2, true).getBody(), StandardCharsets.UTF_8);
            Client client = new PreBuiltTransportClient(
                    Settings.builder().put("cluster.name","docker-cluster").build())
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
            String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(json);
            client.prepareIndex("crawler", "_doc", sha256hex).setSource(json, XContentType.JSON).get();
            channel.close();
            connection.close();
        }
    }

    void request () throws UnknownHostException, ExecutionException, InterruptedException {
        Client client = new PreBuiltTransportClient(
                Settings.builder().put("cluster.name","docker-cluster").build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("AUTHOR_count").field("AUTHOR.keyword");
        SearchSourceBuilder searchSourceBuilder2 = new SearchSourceBuilder().aggregation(aggregationBuilder);
        SearchRequest searchRequest2 = new SearchRequest().indices("crawler").source(searchSourceBuilder2);
        SearchResponse searchResponse = client.search(searchRequest2).get();
        Terms terms = searchResponse.getAggregations().get("AUTHOR_count");

        for (Terms.Bucket bucket : terms.getBuckets())
            log.info("author=" + bucket.getKey()+" count="+bucket.getDocCount());

        client.close();
    }
}

