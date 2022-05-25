package org.example;

public class Json {

    public Json() {
        super();
    }

    public Json(String header, String text, String author, String url, String time) {
        this.HEADER=header;
        this.TEXT=text;
        this.AUTHOR=author;
        this.URL=url;
        this.TIME=time;
    }

    public String HEADER;
    public String TEXT;
    public String AUTHOR;
    public String URL;
    public String TIME;
}