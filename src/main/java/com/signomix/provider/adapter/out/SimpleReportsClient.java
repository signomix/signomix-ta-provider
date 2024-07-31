package com.signomix.provider.adapter.out;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;

import org.jboss.logging.Logger;




public class SimpleReportsClient {

    public static Logger logger = Logger.getLogger(SimpleReportsClient.class);

    public String getReport(String authentication, String query) {
        logger.info("getReport authentication token: " + authentication);
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(20))
                .build();
                try {
                    query =URLEncoder.encode(query, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    return null;
                }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://signomix-reports:8080/api/reports/single?query=" + query))
                .header("Authentication", authentication)
                .GET()
                .build();

        HttpResponse<String> response;
        try {
            response = client.send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return null;
        }
        return response.body();
    }

}
