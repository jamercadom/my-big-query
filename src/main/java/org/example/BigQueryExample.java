package org.example;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

public class BigQueryExample {

    public static void main(String[] args) {
        String keyPath = "/Users/JAMERCADOM/Downloads/new-arrivals-viewer-big-query.json";

        try {
            GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(keyPath));

            BigQuery bigQuery = BigQueryOptions.newBuilder()
                    .setCredentials(credentials)
                    .build()
                    .getService();

            String query = "SELECT sku, enabled FROM `liverpool-big-query.relevance_ranking.NEW_ARRIVALS_HISTORIA` " +
                    "WHERE dia >= '2024-06-01' LIMIT 1000";

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

            JobId jobId = JobId.of(UUID.randomUUID().toString());

            TableResult result = bigQuery.query(queryConfig, jobId);

            result.iterateAll().forEach(row -> {
                String sku = row.get("sku").getStringValue();
                boolean enabled = row.get("enabled").getBooleanValue();
                System.out.println("SKU: " + sku + ", Enabled: " + enabled);
            });

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

