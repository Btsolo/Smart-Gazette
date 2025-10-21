package com.smartgazette.smartgazette.service;

import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.Collections;
import java.util.Map;

@Service
public class IftttWebhookService {

    private final RestTemplate restTemplate = new RestTemplate();

    // IMPORTANT: Replace with your actual IFTTT URL from the Webhooks documentation page
    private final String webhookUrl = "https://maker.ifttt.com/trigger/post_to_x/with/key/dG9GCrRRpnSdRQ2eiK_AEd";

    /**
     * Sends content to the IFTTT Webhook to be posted as a tweet.
     * @param tweetContent The text content of the tweet to post.
     */
    public void postTweet(String tweetContent) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // The JSON body must contain "value1", which corresponds to the ingredient in your IFTTT Applet.
        Map<String, String> body = Collections.singletonMap("value1", tweetContent);

        HttpEntity<Map<String, String>> request = new HttpEntity<>(body, headers);

        try {
            // Send the POST request to IFTTT
            restTemplate.postForObject(webhookUrl, request, String.class);
            System.out.println("Successfully triggered IFTTT webhook to post tweet.");
        } catch (Exception e) {
            // Basic error handling
            System.err.println("Error triggering IFTTT webhook: " + e.getMessage());
            // In a real application, you would use a proper logger here.
        }
    }
}