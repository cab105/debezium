/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that delivers the messages to an HTTP Webhook destination.
 *
 * @author Chris Baumbauer
 *
 */
@Named("http")
@Dependent
public class HttpChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.http.";
    private static final String PROP_WEBHOOK_URL = "url";

    private static String sinkUrl;

    // If this is running as a Knative object, then expect the sink URL to be located in `K_SINK`
    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        String sink = System.getenv("K_SINK");

        if (sink != null) {
            sinkUrl = sink;
        }
        else {
            sinkUrl = config.getValue(PROP_PREFIX + PROP_WEBHOOK_URL, String.class);
        }

        LOGGER.info("Using sink URL: {}", sinkUrl);
    }

    private HttpPost postFactory(boolean cloudevent) {
        HttpPost post;

        post = new HttpPost(sinkUrl);
        if (cloudevent) {
            post.setHeader("content-type", "application/cloudevents+json");
        }
        else {
            post.setHeader("content-type", "application/json");
        }

        // CAB: Add support for basic auth

        return post;
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);
            HttpPost p = postFactory(false);
            String value = "";
            // CAB: Convert this to use reflection
            CloseableHttpClient httpClient = HttpClients.createDefault();

            // Since the ChangeEvent has already been serialized at this point, we need to inspect it to see if this is
            // a cloudevent, and set the correct header
            if (record.value() != null) {
                value = (String) record.value();
                if (value.contains("\"specversion\":\"1.0\"")) {
                    p = postFactory(true);
                }
            }

            try {
                p.setEntity(new StringEntity((String) record.value()));
                HttpResponse response = httpClient.execute(p);

                if (response.getStatusLine().getStatusCode() == 200) {
                    committer.markProcessed(record);
                }

                httpClient.close();
            }
            catch (Exception e) {
                LOGGER.info("exception: " + e.toString());
            }
        }

        committer.markBatchFinished();
    }
}
