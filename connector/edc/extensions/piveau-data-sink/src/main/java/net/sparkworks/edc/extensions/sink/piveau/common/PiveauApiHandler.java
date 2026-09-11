/*
 *  Copyright (c) 2024 SparkWorks
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       SparkWorks - initial implementation
 *
 */

package net.sparkworks.edc.extensions.sink.piveau.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles interaction with the Piveau Hub Repo API.
 * Registers dataset metadata (DCAT-AP JSON-LD) to the Piveau catalog.
 */
public class PiveauApiHandler {
    
    private static final MediaType TURTLE = MediaType.parse("text/turtle");
    private static final int PENDING_RETRY_INTERVAL_SECONDS = 60;
    private static final int PENDING_MAX_RETRIES = 10;

    private final String apiUrl;
    private final String apiKey;
    private final Monitor monitor;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private String daliConnectorUrl;

    private final ConcurrentLinkedQueue<PendingTask> pendingTasks = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService retryScheduler;

    /**
     * The last {@link DatasetMetadata} successfully registered per datasetId — this pipeline
     * only ever gets column/technique/license info from the one JSON metadata file per
     * dataset (there's no separate per-CSV metadata input), so a distribution's own
     * schema:variableMeasured / schema:measurementTechnique / dct:license (see
     * buildDistributionTurtle) are read back from here rather than re-declared per file.
     */
    private final Map<String, DatasetMetadata> datasetMetadataCache = new ConcurrentHashMap<>();

    /**
     * A retryable unit of work submitted to the pending task queue.
     * {@code action.attempt()} returns {@code true} when the task is complete,
     * {@code false} when it should be retried (e.g. a dependency is not yet ready),
     * or throws {@link IOException} on a recoverable error (also retried).
     */
    @FunctionalInterface
    private interface TaskAction {
        boolean attempt() throws IOException;
    }

    private static class PendingTask {
        final String description;
        final TaskAction action;
        final AtomicInteger retryCount = new AtomicInteger(0);

        PendingTask(String description, TaskAction action) {
            this.description = description;
            this.action = action;
        }
    }

    public PiveauApiHandler(String apiUrl, String apiKey, String daliConnectorUrl, Monitor monitor) {
        this.apiUrl = apiUrl;
        this.apiKey = apiKey;
        this.daliConnectorUrl = daliConnectorUrl;
        this.monitor = monitor;
        this.objectMapper = new ObjectMapper();
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

        this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "piveau-pending-retry");
            t.setDaemon(true);
            return t;
        });
        this.retryScheduler.scheduleWithFixedDelay(
                this::retryPendingTasks,
                PENDING_RETRY_INTERVAL_SECONDS,
                PENDING_RETRY_INTERVAL_SECONDS,
                TimeUnit.SECONDS
        );

        monitor.info("PiveauApiHandler initialized");
        monitor.info("  API URL: " + apiUrl);
        monitor.info("  API Key: " + (apiKey != null && !apiKey.isEmpty() ? "***configured***" : "not set"));
        monitor.info("  DALI Connector URL: " + daliConnectorUrl);
        monitor.info("  Pending retry interval: " + PENDING_RETRY_INTERVAL_SECONDS + "s (max " + PENDING_MAX_RETRIES + " attempts)");
    }
    
    /**
     * Handle a JSON file containing dataset metadata.
     * Parses the JSON, transforms it to DCAT-AP Turtle format, and sends to Piveau Hub Repo API.
     *
     * @param filePath    the path to the JSON file
     * @param jsonContent the content of the JSON file with dataset metadata
     * @throws IOException if parsing or API call fails
     */
    public String handleJsonFile(final String datasetId, final String filename, final String catalogueId, Path filePath, String jsonContent) throws IOException {
        monitor.info("Processing dataset metadata from: " + filePath.getFileName());
        
        // Parse JSON input into DatasetMetadata object
        DatasetMetadata metadata = objectMapper.readValue(jsonContent, DatasetMetadata.class);
        
        // Validate required fields
        if (datasetId == null || datasetId.isEmpty()) {
            throw new IOException("Dataset ID is required in JSON metadata");
        }
        // The directory-derived id is the fallback for dct:identifier (and title) when
        // the JSON omits them — see DatasetMetadata.getIdentifier().
        metadata.setDatasetId(datasetId);
        
        // Get current date as fallback for issued; modified defaults to issued date on first creation
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);
        String issuedDate = metadata.getIssued() != null ? metadata.getIssued() : currentDate;
        String modifiedDate = issuedDate;
        
        // Build DCAT-AP Turtle body
        String turtleBody = buildDcatTurtle(datasetId, metadata, issuedDate, modifiedDate);
        
        monitor.info(turtleBody);
        
        monitor.debug("Generated DCAT-AP Turtle:\n" + turtleBody);
        
        // Build the URL with catalogue query parameter if provided
        String url = apiUrl + "/" + datasetId + "?catalogue=" + catalogueId;
        
        // Build the HTTP request
        RequestBody requestBody = RequestBody.create(turtleBody, TURTLE);
        Request.Builder requestBuilder = new Request.Builder().url(url).put(requestBody).header("Content-Type", "text/turtle").header("Accept", "application/json");
        
        // Add API key if configured
        if (apiKey != null && !apiKey.isEmpty()) {
            requestBuilder.header("X-API-Key", apiKey);
        }
        
        Request request = requestBuilder.build();
        
        monitor.info("Sending DCAT-AP dataset to Piveau Hub Repo: ");
        monitor.info("  ApiUrl: " + apiUrl);
        monitor.info("  CatalogueId: " + catalogueId);
        monitor.info("  Dataset ID: " + datasetId);
        
        // Execute the request
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "";
                monitor.info("✓ Dataset registered successfully in Piveau Hub");
                monitor.info("  Response code: " + response.code());
                if (!responseBody.isEmpty()) {
                    monitor.debug("  Response body: " + responseBody);
                }
                datasetMetadataCache.put(datasetId, metadata);
                return datasetId;
            } else {
                String errorBody = response.body() != null ? response.body().string() : "No error details";
                String errorMessage = String.format("Failed to register dataset in Piveau Hub (HTTP %d): %s", response.code(), errorBody);
                monitor.severe("✗ " + errorMessage);
                throw new IOException(errorMessage);
            }
        } catch (IOException e) {
            monitor.severe("✗ Failed to communicate with Piveau Hub Repo API", e);
            throw e;
        }
    }
    
    /**
     * Build DCAT-AP / 6G-DALI Turtle representation of the dataset,
     * compliant with the 6G-DALI Metadata Application Profile v1.0.
     */
    private String buildDcatTurtle(String datasetId, DatasetMetadata metadata, String issuedDate, String modifiedDate) {
        StringBuilder turtle = new StringBuilder();

        // Prefixes (alphabetical)
        turtle.append("@prefix adms:   <http://www.w3.org/ns/adms#> .\n");
        turtle.append("@prefix dali:   <https://dali-project.eu/ns#> .\n");
        turtle.append("@prefix dcat:   <http://www.w3.org/ns/dcat#> .\n");
        turtle.append("@prefix dct:    <http://purl.org/dc/terms/> .\n");
        turtle.append("@prefix foaf:   <http://xmlns.com/foaf/0.1/> .\n");
        turtle.append("@prefix gax:    <https://registry.lab.gaia-x.eu/v1/api/trusted-shape-registry/v1/shapes/jsonld/trustframework#> .\n");
        turtle.append("@prefix prov:   <http://www.w3.org/ns/prov#> .\n");
        turtle.append("@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
        turtle.append("@prefix schema: <https://schema.org/> .\n");
        turtle.append("@prefix skos:   <http://www.w3.org/2004/02/skos/core#> .\n");
        turtle.append("@prefix vcard:  <http://www.w3.org/2006/vcard/ns#> .\n");
        turtle.append("@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .\n\n");

        String datasetUri = apiUrl + "/" + datasetId;

        turtle.append("<").append(datasetUri).append(">\n");

        // ── Types ──────────────────────────────────────────────────────────────
        turtle.append("    a                         dcat:Dataset, gax:DataResource ;\n");

        // ── Core identity (Mandatory) ──────────────────────────────────────────
        turtle.append("    dct:title                 \"").append(escapeString(metadata.getTitle())).append("\"@en ;\n");
        turtle.append("    dct:description           \"").append(escapeString(metadata.getDescription())).append("\"@en ;\n");
        turtle.append("    dct:identifier            \"").append(escapeString(metadata.getIdentifier())).append("\" ;\n");
        turtle.append("    dct:issued                \"").append(issuedDate).append("\"^^xsd:date ;\n");
        turtle.append("    dct:modified              \"").append(modifiedDate).append("\"^^xsd:date ;\n");
        turtle.append("    dct:language              <http://publications.europa.eu/resource/authority/language/")
              .append(metadata.getLanguage()).append("> ;\n");

        // Version (Recommended, optional)
        if (hasText(metadata.getVersion())) {
            turtle.append("    adms:version              \"").append(escapeString(metadata.getVersion())).append("\" ;\n");
        }

        // ── Coverage (Recommended) ─────────────────────────────────────────────
        if (hasText(metadata.getSpatial())) {
            turtle.append("    dct:spatial               \"").append(escapeString(metadata.getSpatial())).append("\" ;\n");
        }
        if (hasText(metadata.getTemporalStart()) && hasText(metadata.getTemporalEnd())) {
            turtle.append("    dct:temporal              [ a dct:PeriodOfTime ; dcat:startDate \"")
                  .append(metadata.getTemporalStart()).append("\"^^xsd:date ; dcat:endDate \"")
                  .append(metadata.getTemporalEnd()).append("\"^^xsd:date ] ;\n");
        }

        // ── Rights & License (Mandatory) ───────────────────────────────────────
        turtle.append("    dct:accessRights          <http://publications.europa.eu/resource/authority/access-right/")
              .append(metadata.getAccessRights()).append("> ;\n");
        turtle.append("    dct:license               <").append(metadata.getLicense()).append("> ;\n");
        turtle.append("    dct:conformsTo            <https://www.go-fair.org/fair-principles/> ;\n");

        // ── SNS-JU / DALI (Mandatory) ──────────────────────────────────────────
        turtle.append("    dali:snsProjectName       \"").append(escapeString(metadata.getSnsProjectName())).append("\" ;\n");
        turtle.append("    dali:gdprCompliant        ").append(metadata.isGdprCompliant()).append(" ;\n");
        turtle.append("    dali:fairCompliant        ").append(metadata.isFairCompliant()).append(" ;\n");

        // ── GAIA-X (Mandatory) ─────────────────────────────────────────────────
        turtle.append("    gax:containsPII           ").append(metadata.isContainsPii()).append(" ;\n");
        if (hasText(metadata.getProducedBy())) {
            turtle.append("    gax:producedBy            <").append(metadata.getProducedBy()).append("> ;\n");
            turtle.append("    prov:wasAttributedTo      <").append(metadata.getProducedBy()).append("> ;\n");
        }
        if (hasText(metadata.getExposedThrough())) {
            turtle.append("    gax:exposedThrough        <").append(metadata.getExposedThrough()).append("> ;\n");
        }

        // ── Classification (Recommended) ───────────────────────────────────────
        turtle.append("    dcat:theme                <http://publications.europa.eu/resource/authority/data-theme/")
              .append(metadata.getTheme()).append("> ;\n");

        String keywords = formatKeywords(metadata.getKeywords());
        if (keywords != null && !keywords.isEmpty()) {
            turtle.append("    dcat:keyword              ").append(keywords).append(" ;\n");
        }

        // ── Agents (Recommended) ───────────────────────────────────────────────
        if (hasText(metadata.getPublisher())) {
            turtle.append("    dct:publisher             [ a foaf:Organization ; foaf:name \"")
                  .append(escapeString(metadata.getPublisher())).append("\" ] ;\n");
        }

        if (hasText(metadata.getCreatorName())) {
            // foaf:Person for named researchers, foaf:Organization when the institution
            // itself is credited — the MAP uses both. ORCID goes in schema:identifier as
            // an IRI, so a bare "0000-0002-..." is expanded to its orcid.org form.
            String nodeType = "Organization".equalsIgnoreCase(metadata.getCreatorKind()) ? "foaf:Organization" : "foaf:Person";
            turtle.append("    dct:creator               [ a ").append(nodeType)
                  .append(" ; foaf:name \"").append(escapeString(metadata.getCreatorName())).append("\"");
            if (hasText(metadata.getCreatorEmail())) {
                turtle.append(" ; foaf:mbox <mailto:").append(metadata.getCreatorEmail()).append(">");
            }
            if (hasText(metadata.getCreatorOrcid())) {
                String orcid = metadata.getCreatorOrcid().trim();
                if (!orcid.startsWith("http")) {
                    orcid = "https://orcid.org/" + orcid;
                }
                turtle.append(" ; schema:identifier <").append(orcid).append(">");
            }
            if (hasText(metadata.getCreatorAffiliation())) {
                turtle.append(" ; schema:affiliation \"").append(escapeString(metadata.getCreatorAffiliation())).append("\"");
            }
            turtle.append(" ] ;\n");
        }

        for (String contributor : metadata.getContributors()) {
            if (!hasText(contributor)) continue;
            turtle.append("    dct:contributor           [ a foaf:Agent ; foaf:name \"")
                  .append(escapeString(contributor)).append("\" ] ;\n");
        }

        for (String publication : metadata.getRelatedPublications()) {
            if (!hasText(publication)) continue;
            turtle.append("    dct:relation              <").append(publication.trim()).append("> ;\n");
        }

        if (hasText(metadata.getContactEmail())) {
            turtle.append("    dcat:contactPoint         [ a vcard:Organization ; vcard:hasEmail <mailto:")
                  .append(metadata.getContactEmail()).append("> ] ;\n");
        }

        // Note: schema:variableMeasured / schema:measurementTechnique describe a specific
        // file's columns, not the dataset as a whole (a dataset can have distributions with
        // different columns) — per the 6G-DALI MAP (§5.3.E) these belong on the
        // dcat:Distribution instead (see buildDistributionTurtle), not here.

        // ── 5G/6G Testbed Context (Recommended) ───────────────────────────────
        TestbedContext tc = metadata.getTestbedContext();
        if (tc != null) {
            turtle.append("    dali:testbedContext       [\n");
            turtle.append("        a                     dali:TestbedContext ;\n");
            appendTcStringProp(turtle, "dali:underlayPlatform",         tc.getUnderlayPlatform(), true);
            appendTcStringProp(turtle, "dali:environment",              tc.getEnvironment(), false);
            appendTcStringProp(turtle, "dali:networkDomain",            tc.getNetworkDomain(), false);
            appendTcStringProp(turtle, "dali:ran3gppRelease",           tc.getRan3gppRelease(), false);
            appendTcStringProp(turtle, "dali:ranNewRadioType",          tc.getRanNewRadioType(), false);
            appendTcStringProp(turtle, "dali:ranSplit",                 tc.getRanSplit(), false);
            appendTcStringProp(turtle, "dali:ranFocusedTechnology",     tc.getRanFocusedTechnology(), false);
            appendTcStringProp(turtle, "dali:ranCoverageType",          tc.getRanCoverageType(), false);
            appendTcListProp(turtle,   "dali:ranFrequencyBand",         tc.getRanFrequencyBand());
            if (tc.getRanBandwidthMHz() != null) {
                turtle.append("        dali:ranBandwidthMHz      ").append(tc.getRanBandwidthMHz()).append(" ;\n");
            }
            if (tc.getRanMaxEndDevices() != null) {
                turtle.append("        dali:ranMaxEndDevices     ").append(tc.getRanMaxEndDevices()).append(" ;\n");
            }
            appendTcStringProp(turtle, "dali:ranMobilityModel",         tc.getRanMobilityModel(), false);
            appendTcStringProp(turtle, "dali:coreRelease",              tc.getCoreRelease(), false);
            appendTcStringProp(turtle, "dali:coreSolution",             tc.getCoreSolution(), false);
            appendTcStringProp(turtle, "dali:transportType",            tc.getTransportType(), false);
            appendTcStringProp(turtle, "dali:computeOrchestratorType",  tc.getComputeOrchestratorType(), false);
            if (tc.getComputeGpuUse() != null) {
                turtle.append("        dali:computeGpuUse        ")
                      .append(tc.getComputeGpuUse()).append("^^xsd:boolean ;\n");
            }
            appendTcStringProp(turtle, "dali:computeVirtualizationType", tc.getComputeVirtualizationType(), false);
            appendTcStringProp(turtle, "dali:computeInfrastructureType", tc.getComputeInfrastructureType(), false);
            appendTcStringProp(turtle, "dali:trafficOrigin",            tc.getTrafficOrigin(), false);
            appendTcStringProp(turtle, "dali:trafficPattern",           tc.getTrafficPattern(), false);
            appendTcStringProp(turtle, "dali:sliceType",                tc.getSliceType(), false);
            appendTcStringProp(turtle, "dali:referencePlane",           tc.getReferencePlane(), false);
            appendTcStringProp(turtle, "dali:relatedVertical",          tc.getRelatedVertical(), false);
            appendTcStringProp(turtle, "dali:observationPointHorizontal", tc.getObservationPointHorizontal(), false);
            appendTcStringProp(turtle, "dali:observationPointVertical", tc.getObservationPointVertical(), false);
            appendTcListProp(turtle,   "dali:measurementFamily",        tc.getMeasurementFamily());
            appendTcListProp(turtle,   "dali:measurementTool",          tc.getMeasurementTool());
            turtle.append("    ] ;\n");
        }

        // Replace the final " ;\n" with " .\n" to close the subject block
        String result = turtle.toString();
        int lastSemi = result.lastIndexOf(" ;\n");
        if (lastSemi >= 0) {
            result = result.substring(0, lastSemi) + " .\n" + result.substring(lastSemi + 3);
        }
        return result;
    }

    private boolean hasText(String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Append a single-value predicate to a testbed context blank node.
     * @param isUri when true the value is emitted as a URI {@code <value>}, otherwise as a string literal
     */
    private void appendTcStringProp(StringBuilder sb, String predicate, String value, boolean isUri) {
        if (!hasText(value)) return;
        sb.append("        ").append(predicate).append("  ");
        if (isUri) {
            sb.append("<").append(value).append(">");
        } else {
            sb.append("\"").append(escapeString(value)).append("\"");
        }
        sb.append(" ;\n");
    }

    /**
     * Append a repeatable predicate to a testbed context blank node, one triple per value.
     * Blank entries are skipped so an unfilled list row is not published as an empty literal.
     */
    private void appendTcListProp(StringBuilder sb, String predicate, List<String> values) {
        if (values == null) return;
        for (String value : values) {
            if (value == null || value.isBlank()) continue;
            sb.append("        ").append(predicate).append("  \"")
              .append(escapeString(value.trim())).append("\" ;\n");
        }
    }
    
    /**
     * Format keywords list into comma-separated quoted strings for Turtle.
     */
    private String formatKeywords(List<String> keywordsList) {
        if (keywordsList == null || keywordsList.isEmpty()) {
            return "";
        }
        
        StringBuilder keywords = new StringBuilder();
        for (int i = 0; i < keywordsList.size(); i++) {
            if (i > 0) {
                keywords.append(", ");
            }
            keywords.append("\"").append(escapeString(keywordsList.get(i))).append("\"");
        }
        return keywords.toString();
    }
    
    /**
     * Escape special characters in strings for Turtle format.
     */
    private String escapeString(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t");
    }
    
    /**
     * Test the connection to the Piveau Hub Repo API.
     *
     * @return true if the API is reachable, false otherwise
     */
    public boolean testConnection() {
        try {
            Request.Builder requestBuilder = new Request.Builder().url(apiUrl).head();  // HEAD request to check connectivity
            
            if (apiKey != null && !apiKey.isEmpty()) {
                requestBuilder.header("X-API-Key", apiKey);
            }
            
            Request request = requestBuilder.build();
            
            try (Response response = httpClient.newCall(request).execute()) {
                boolean isSuccessful = response.isSuccessful() || response.code() == 405; // 405 = Method Not Allowed is OK for HEAD
                if (isSuccessful) {
                    monitor.info("✓ Piveau Hub Repo API is reachable");
                } else {
                    monitor.warning("⚠ Piveau Hub Repo API returned status: " + response.code());
                }
                return isSuccessful;
            }
        } catch (Exception e) {
            monitor.warning("⚠ Failed to connect to Piveau Hub Repo API: " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Check whether a dataset is already registered in the Piveau Hub Repo.
     *
     * @param datasetId the dataset ID to look up
     * @return true if the dataset exists (HTTP 200), false otherwise
     */
    public boolean datasetExists(String datasetId, String catalogueId) {
        if (!hasText(apiUrl) || !hasText(datasetId)) {
            return false;
        }
        String url = apiUrl + "/" + datasetId + "?catalogue=" + catalogueId;
        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .get()
                .header("Accept", "text/turtle");
        if (hasText(apiKey)) {
            requestBuilder.header("X-API-Key", apiKey);
        }
        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            boolean exists = response.code() == 200;
            monitor.info(exists
                    ? "✓ Dataset '" + datasetId + "' is registered in Piveau"
                    : "⚠ Dataset '" + datasetId + "' is NOT registered in Piveau (HTTP " + response.code() + ")");
            return exists;
        } catch (IOException e) {
            monitor.warning("⚠ Could not check dataset existence in Piveau: " + e.getMessage());
            return false;
        }
    }

    /**
     * Enqueue a JSON metadata file whose Piveau dataset registration failed.
     * The task returns {@code true} on success and throws on API error (both cases handled by the retry loop).
     */
    public void schedulePendingDataset(String datasetId, String fileName, String catalogueId, String jsonContent) {
        monitor.info("⏳ Queuing dataset registration for '" + datasetId
                + "' — will retry every " + PENDING_RETRY_INTERVAL_SECONDS + "s");
        pendingTasks.add(new PendingTask(
                "Dataset registration: " + datasetId,
                () -> {
                    handleJsonFile(datasetId, fileName, catalogueId, Path.of(fileName), jsonContent);
                    return true;
                }
        ));
    }

    /**
     * Enqueue a CSV file whose distribution creation is waiting for its parent dataset to be registered.
     * The task returns {@code false} (retry) while the dataset is absent, and {@code true} once
     * the distribution is successfully created.
     */
    public void schedulePendingDistribution(String datasetId, String fileName, String catalogueId, String downloadUrl) {
        monitor.info("⏳ Queuing distribution for '" + fileName + "' under dataset '" + datasetId
                + "' (catalogue: " + catalogueId + ") — will retry every " + PENDING_RETRY_INTERVAL_SECONDS + "s");
        pendingTasks.add(new PendingTask(
                "Distribution: " + fileName + " → dataset " + datasetId,
                () -> {
                    if (!datasetExists(datasetId, catalogueId)) return false;
                    createDistribution(datasetId, fileName, downloadUrl);
                    return true;
                }
        ));
    }

    /**
     * Periodic retry task: drains the pending queue and re-executes each task.
     * Tasks that return {@code false} or throw are re-queued up to {@value #PENDING_MAX_RETRIES} times.
     */
    private void retryPendingTasks() {
        if (pendingTasks.isEmpty()) return;
        monitor.info("🔄 Retrying " + pendingTasks.size() + " pending task(s)...");

        List<PendingTask> snapshot = new ArrayList<>(pendingTasks);
        pendingTasks.clear();

        for (PendingTask task : snapshot) {
            int attempt = task.retryCount.incrementAndGet();
            if (attempt > PENDING_MAX_RETRIES) {
                monitor.warning("⚠ Giving up after " + PENDING_MAX_RETRIES + " attempts: " + task.description);
                continue;
            }
            try {
                boolean done = task.action.attempt();
                if (done) {
                    monitor.info("✓ Completed (attempt " + attempt + "): " + task.description);
                } else {
                    monitor.info("⏳ Not ready yet (attempt " + attempt + "/" + PENDING_MAX_RETRIES
                            + "): " + task.description + " — re-queuing");
                    pendingTasks.add(task);
                }
            } catch (IOException e) {
                monitor.warning("⚠ Attempt " + attempt + "/" + PENDING_MAX_RETRIES + " failed: "
                        + task.description + ": " + e.getMessage() + " — re-queuing");
                pendingTasks.add(task);
            }
        }
    }

    /**
     * Create a distribution for a file in an existing Piveau dataset.
     * The distribution represents the actual data file (CSV, etc.) associated with the dataset.
     *
     * @param datasetId   the ID of the dataset (from dirName)
     * @param fileName    the name of the file being uploaded
     * @param downloadUrl the file's actual S3/MinIO URL, written as dcat:downloadURL —
     *                    distinct from dcat:accessURL (the EDC connector's negotiation
     *                    entrypoint, always set). May be null (e.g. the HTTP-forward
     *                    fallback path never places the file in S3, so there's nothing
     *                    to record here).
     * @return the distribution ID
     * @throws IOException if the API call fails
     */
    public String createDistribution(String datasetId, String fileName, String downloadUrl) throws IOException {
        if (datasetId == null || datasetId.isEmpty()) {
            throw new IOException("Dataset ID is required to create distribution");
        }

        if (fileName == null || fileName.isEmpty()) {
            throw new IOException("File name is required to create distribution");
        }

        // Generate distribution ID from filename (remove extension and sanitize) — used only
        // for the distribution's own URI slug (piveau mints its own canonical id on write
        // regardless), so it's kept dataset-prefixed for readability/uniqueness there.
        String distributionId = generateDistributionId(datasetId + "-" + fileName);

        // dali:assetId, by contrast, must match the id the file is (or will be) registered
        // under as an EDC asset — s3-asset-monitor's EdcAssetRegistrationService.deriveAssetId
        // uses the bare filename with its extension stripped (no dataset prefix, no
        // slugification), so this has to use the exact same derivation to cross-reference.
        String assetId = deriveAssetId(fileName);

        monitor.info("Creating distribution for dataset: " + datasetId  + " filename: "+ fileName);
        monitor.info("  DistributionId: " + distributionId);
        monitor.info("  AssetId: " + assetId);

        // Get current date for issued/modified
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

        // Detect format and media type from file extension
        String format = detectFormat(fileName);
        String mediaType = detectMediaType(fileName);

        // Columns / measurement technique / license come from the dataset's own JSON
        // metadata (there's no separate per-CSV metadata input in this pipeline) — cached
        // in handleJsonFile since it's only available at dataset-registration time, well
        // before any of its distributions exist.
        DatasetMetadata datasetMetadata = datasetMetadataCache.get(datasetId);
        List<String> columns = datasetMetadata != null ? datasetMetadata.getColumns() : null;
        String measurementTechnique = datasetMetadata != null ? datasetMetadata.getMeasurementTechnique() : null;
        String license = datasetMetadata != null && hasText(datasetMetadata.getLicense())
                ? datasetMetadata.getLicense()
                : "https://creativecommons.org/licenses/by/4.0/";

        // Build DCAT-AP Turtle body for the distribution
        String turtleBody = buildDistributionTurtle(datasetId, distributionId, assetId, fileName, format, mediaType,
                currentDate, downloadUrl, columns, measurementTechnique, license);

        monitor.info("Generated Distribution Turtle:\n" + turtleBody);

        // Build the URL for distribution creation
        String url = apiUrl + "/" + datasetId + "/distributions";

        // Build the HTTP request
        RequestBody requestBody = RequestBody.create(turtleBody, TURTLE);
        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .post(requestBody)
                .header("Content-Type", "text/turtle")
                .header("Accept", "application/json");

        // Add API key if configured
        if (apiKey != null && !apiKey.isEmpty()) {
            requestBuilder.header("X-API-Key", apiKey);
        }

        Request request = requestBuilder.build();

        monitor.info("Sending distribution to Piveau Hub Repo: " + url);

        // Execute the request
        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                String responseBody = response.body() != null ? response.body().string() : "";
                monitor.info("✓ Distribution created successfully in Piveau Hub");
                monitor.info("  Response code: " + response.code());
                monitor.info("  Distribution ID: " + distributionId);
                if (!responseBody.isEmpty()) {
                    monitor.debug("  Response body: " + responseBody);
                }
                return distributionId;
            } else {
                String errorBody = response.body() != null ? response.body().string() : "No error details";
                String errorMessage = String.format(
                        "Failed to create distribution in Piveau Hub (HTTP %d): %s",
                        response.code(),
                        errorBody
                );
                monitor.severe("✗ " + errorMessage);
                throw new IOException(errorMessage);
            }
        } catch (IOException e) {
            monitor.severe("✗ Failed to communicate with Piveau Hub Repo API", e);
            throw e;
        }
    }

    /**
     * Build DCAT-AP / 6G-DALI Turtle representation of a distribution,
     * compliant with the 6G-DALI Metadata Application Profile v1.0.
     */
    private String buildDistributionTurtle(String datasetId, String distributionId, String assetId,
                                          String fileName, String format,
                                          String mediaType, String issuedDate, String downloadUrl,
                                          List<String> columns, String measurementTechnique, String license) {
        StringBuilder turtle = new StringBuilder();

        // Prefixes
        turtle.append("@prefix dali:   <https://dali-project.eu/ns#> .\n");
        turtle.append("@prefix dcat:   <http://www.w3.org/ns/dcat#> .\n");
        turtle.append("@prefix dcatap: <http://data.europa.eu/r5r/> .\n");
        turtle.append("@prefix dct:    <http://purl.org/dc/terms/> .\n");
        turtle.append("@prefix schema: <https://schema.org/> .\n");
        turtle.append("@prefix xsd:    <http://www.w3.org/2001/XMLSchema#> .\n\n");

        String distributionUri = apiUrl + "/" + datasetId + "/distributions/" + distributionId;

        turtle.append("<").append(distributionUri).append(">\n");
        turtle.append("    a                       dcat:Distribution ;\n");
        turtle.append("    dct:title               \"").append(escapeString(fileName)).append("\"@en ;\n");
        turtle.append("    dct:description         \"Data distribution for ").append(escapeString(fileName)).append("\"@en ;\n");
        // accessURL is the EDC connector's negotiation entrypoint (this distribution is
        // registered there under dali:assetId — see s3-asset-monitor's
        // EdcAssetRegistrationService), not the raw file — that's downloadURL below.
        turtle.append("    dcat:accessURL          <").append(daliConnectorUrl).append("> ;\n");
        if (hasText(downloadUrl)) {
            turtle.append("    dcat:downloadURL        <").append(downloadUrl).append("> ;\n");
        }
        turtle.append("    dali:assetId            \"").append(escapeString(assetId)).append("\" ;\n");
        turtle.append("    dali:connectorType      \"dspaceconnector\" ;\n");
        turtle.append("    dct:format              \"").append(format).append("\" ;\n");
        turtle.append("    dcat:mediaType          \"").append(mediaType).append("\" ;\n");
        // schema:variableMeasured / schema:measurementTechnique describe this specific
        // file's columns, not the dataset as a whole (see buildDcatTurtle) — per 6G-DALI
        // MAP §5.3.E these belong here, on the distribution.
        if (columns != null && !columns.isEmpty()) {
            turtle.append("    schema:variableMeasured ");
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) turtle.append(", ");
                turtle.append("\"").append(escapeString(columns.get(i))).append("\"");
            }
            turtle.append(" ;\n");
        }
        if (hasText(measurementTechnique)) {
            turtle.append("    schema:measurementTechnique \"")
                  .append(escapeString(measurementTechnique)).append("\"@en ;\n");
        }
        // Mirrors the dataset's own declared license (see buildDcatTurtle) rather than a
        // hardcoded value independent of what the dataset submitter actually chose.
        turtle.append("    dct:license             <").append(license).append("> ;\n");
        turtle.append("    dct:issued              \"").append(issuedDate).append("\"^^xsd:date ;\n");
        turtle.append("    dcatap:availability     <http://data.europa.eu/r5r/availability/STABLE> .\n");

        return turtle.toString();
    }

    /**
     * Register an asset to the DALI EDC connector's Management API.
     *
     * @param assetId     the asset ID to register
     * @param dataAddress map of data address properties (must include "type")
     * @throws IOException if the API call fails
     */
    public void registerAsset(String assetId, Map<String, String> dataAddress) throws IOException {
        if (daliConnectorUrl == null || daliConnectorUrl.isEmpty()) {
            monitor.warning("⚠ DALI connector URL not configured, skipping asset registration");
            return;
        }

        Map<String, Object> asset = new HashMap<>();
        asset.put("@context", Map.of("@vocab", "https://w3id.org/edc/v0.0.1/ns/"));
        asset.put("@id", assetId);
        asset.put("properties", Map.of());
        asset.put("dataAddress", dataAddress);

        String jsonBody = objectMapper.writeValueAsString(asset);
        String url = daliConnectorUrl + "/management/v3/assets";

        RequestBody requestBody = RequestBody.create(jsonBody, MediaType.parse("application/json"));
        Request request = new Request.Builder()
            .url(url)
            .post(requestBody)
            .header("Content-Type", "application/json")
            .build();

        monitor.info("Registering asset '" + assetId + "' to DALI connector: " + url);

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                monitor.info("✓ Asset '" + assetId + "' registered to DALI connector (status: " + response.code() + ")");
            } else {
                String errorBody = response.body() != null ? response.body().string() : "No error details";
                throw new IOException(String.format("Failed to register asset '%s' (HTTP %d): %s", assetId, response.code(), errorBody));
            }
        } catch (IOException e) {
            monitor.severe("✗ Failed to register asset to DALI connector", e);
            throw e;
        }
    }

    /**
     * Generate a distribution ID from a filename.
     * Removes file extension and sanitizes the name.
     */
    private String generateDistributionId(String fileName) {
        // Remove file extension
        int lastDot = fileName.lastIndexOf('.');
        String baseName = lastDot > 0 ? fileName.substring(0, lastDot) : fileName;

        // Sanitize: replace non-alphanumeric characters with hyphens
        return baseName.toLowerCase()
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("^-+|-+$", ""); // Remove leading/trailing hyphens
    }

    /**
     * The EDC asset id a given file is (or will be) registered under — must exactly match
     * s3-asset-monitor's EdcAssetRegistrationService.deriveAssetId: just the filename with
     * its extension stripped, no path, no slugification. Kept separate from
     * generateDistributionId (which sanitizes/prefixes for the distribution's own URI slug)
     * since the two ids serve different purposes and must not be conflated.
     */
    private String deriveAssetId(String fileName) {
        if (fileName == null) return "unknown";
        int lastSlash = Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\'));
        String baseName = lastSlash >= 0 ? fileName.substring(lastSlash + 1) : fileName;
        int lastDot = baseName.lastIndexOf('.');
        return lastDot > 0 ? baseName.substring(0, lastDot) : baseName;
    }

    /**
     * Detect format from file extension.
     */
    private String detectFormat(String fileName) {
        String extension = getFileExtension(fileName).toLowerCase();
        switch (extension) {
            case "csv":
                return "CSV";
            case "tsv":
                return "TSV";
            case "json":
                return "JSON";
            case "jsonld":
                return "JSON-LD";
            case "jsonl":
            case "ndjson":
                return "JSONL";
            case "txt":
                return "TXT";
            case "xml":
                return "XML";
            case "parquet":
                return "PARQUET";
            case "xlsx":
            case "xls":
                return "XLSX";
            case "pdf":
                return "PDF";
            default:
                return extension.toUpperCase();
        }
    }

    /**
     * Detect media type from file extension — kept in sync with the canonical extension
     * mappings used elsewhere in this project (dataops-orchestrator/piveau_dataset_client.py's
     * CANONICAL_MEDIA_TYPE_BY_EXTENSION, s3-asset-monitor's EdcAssetRegistrationService).
     */
    private String detectMediaType(String fileName) {
        String extension = getFileExtension(fileName).toLowerCase();
        switch (extension) {
            case "csv":
                return "text/csv";
            case "tsv":
                return "text/tab-separated-values";
            case "jsonl":
                return "application/jsonl";
            case "ndjson":
                return "application/x-ndjson";
            case "json":
                return "application/json";
            case "jsonld":
                return "application/ld+json";
            case "txt":
                return "text/plain";
            case "xml":
                return "application/xml";
            case "parquet":
                return "application/parquet";
            case "xlsx":
                return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            case "xls":
                return "application/vnd.ms-excel";
            case "pdf":
                return "application/pdf";
            default:
                return "application/octet-stream";
        }
    }

    /**
     * Get file extension from filename.
     */
    private String getFileExtension(String fileName) {
        int lastDot = fileName.lastIndexOf('.');
        return lastDot > 0 ? fileName.substring(lastDot + 1) : "";
    }
}
