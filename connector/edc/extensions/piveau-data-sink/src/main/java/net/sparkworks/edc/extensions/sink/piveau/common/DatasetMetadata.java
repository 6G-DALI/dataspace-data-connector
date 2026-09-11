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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents dataset metadata from JSON input that will be transformed to DCAT-AP / 6G-DALI format.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatasetMetadata {

    // ── Core identity ─────────────────────────────────────────────────────────

    private String datasetId;

    /**
     * Explicit {@code dct:identifier} value. Falls back to {@code datasetId} when absent.
     * Suppressed so the custom getter below is used.
     */
    @Getter(AccessLevel.NONE)
    private String identifier;

    @Getter(AccessLevel.NONE)
    private String title;

    @Getter(AccessLevel.NONE)
    private String description;

    /** Dataset version string. Maps to {@code adms:version}. */
    private String version;

    private String issued;

    private String modified;

    /** Geographic coverage, free text. Maps to {@code dct:spatial}. */
    private String spatial;

    /** Start of the covered period ({@code YYYY-MM-DD}). Maps to {@code dct:temporal dcat:startDate}. */
    @JsonProperty("temporal_start")
    @JsonAlias("temporalStart")
    private String temporalStart;

    /** End of the covered period ({@code YYYY-MM-DD}). Maps to {@code dct:temporal dcat:endDate}. */
    @JsonProperty("temporal_end")
    @JsonAlias("temporalEnd")
    private String temporalEnd;

    // ── Classification ────────────────────────────────────────────────────────

    /** EU Data Theme code (default: TECH). Maps to {@code dcat:theme}. */
    private String theme = "TECH";

    @Setter(AccessLevel.NONE)
    private List<String> keywords = new ArrayList<>();

    /**
     * Language code from EU MDR Languages vocabulary (default: ENG).
     * Maps to {@code dct:language}.
     */
    private String language = "ENG";

    // ── Rights & license ─────────────────────────────────────────────────────

    /** License URI (default: CC-BY-4.0). Maps to {@code dct:license}. */
    private String license = "https://creativecommons.org/licenses/by/4.0/";

    /**
     * EU Access Right vocabulary code (default: PUBLIC).
     * Maps to {@code dct:accessRights}.
     */
    @JsonProperty("access_rights")
    @JsonAlias("accessRights")
    private String accessRights = "PUBLIC";

    // ── Compliance (MAP §5.2 / CMT Group 2) ──────────────────────────────────

    /** Owner's GDPR compliance statement. Maps to {@code dali:gdprCompliant}. */
    @JsonProperty("gdpr_compliant")
    @JsonAlias("gdprCompliant")
    private boolean gdprCompliant = true;

    /** Owner's FAIR compliance statement. Maps to {@code dali:fairCompliant}. */
    @JsonProperty("fair_compliant")
    @JsonAlias("fairCompliant")
    private boolean fairCompliant = true;

    /** Whether the data contains personally identifiable information. Maps to {@code gax:containsPII}. */
    @JsonProperty("contains_pii")
    @JsonAlias("containsPii")
    private boolean containsPii = false;

    /**
     * URI of the {@code gax:DataExchangeComponent} (data space endpoint) the dataset is
     * accessible through. Maps to {@code gax:exposedThrough}; omitted when unset.
     */
    @JsonProperty("exposed_through")
    @JsonAlias("exposedThrough")
    private String exposedThrough;

    // ── Agents ───────────────────────────────────────────────────────────────

    /** Publishing organisation name. Maps to {@code dct:publisher foaf:name}. */
    private String publisher;

    /** Creator's full name. Maps to {@code dct:creator foaf:name}. */
    @JsonProperty("creator_name")
    @JsonAlias("creatorName")
    private String creatorName;

    /** Creator's e-mail address. Maps to {@code dct:creator foaf:mbox}. */
    @JsonProperty("creator_email")
    @JsonAlias("creatorEmail")
    private String creatorEmail;

    /** Creator's ORCID (bare id or URL). Maps to {@code dct:creator schema:identifier}. */
    @JsonProperty("creator_orcid")
    @JsonAlias("creatorOrcid")
    private String creatorOrcid;

    /** Creator's affiliation. Maps to {@code dct:creator schema:affiliation}. */
    @JsonProperty("creator_affiliation")
    @JsonAlias("creatorAffiliation")
    private String creatorAffiliation;

    /**
     * Whether the creator is an institution rather than a person: {@code Person} (default)
     * or {@code Organization}. Selects the {@code rdf:type} of the {@code dct:creator} node.
     */
    @JsonProperty("creator_kind")
    @JsonAlias("creatorKind")
    private String creatorKind = "Person";

    /** Names of further contributors. Each maps to a {@code dct:contributor foaf:Agent}. */
    @Setter(AccessLevel.NONE)
    private List<String> contributors = new ArrayList<>();

    /** URIs (DOI, arXiv, ...) of related publications. Each maps to {@code dct:relation}. */
    @JsonProperty("related_publications")
    @JsonAlias("relatedPublications")
    @Setter(AccessLevel.NONE)
    private List<String> relatedPublications = new ArrayList<>();

    /** Contact e-mail for the dataset. Maps to {@code dcat:contactPoint vcard:hasEmail}. */
    @JsonProperty("contact_email")
    @JsonAlias("contactEmail")
    private String contactEmail;

    /**
     * URI of the GAIA-X legal participant that produced the dataset (e.g. a testbed partner URI).
     * Maps to {@code gax:producedBy} and {@code prov:wasAttributedTo}.
     */
    @JsonProperty("produced_by")
    @JsonAlias("producedBy")
    private String producedBy;

    // ── SNS-JU / DALI ────────────────────────────────────────────────────────

    /** SNS-JU project name (default: 6G-DALI). Maps to {@code dali:snsProjectName}. */
    @JsonProperty("sns_project_name")
    @JsonAlias("snsProjectName")
    private String snsProjectName = "6G-DALI";

    // ── Content description ───────────────────────────────────────────────────

    /**
     * Column / variable names for the dataset.
     * Maps to {@code schema:variableMeasured}.
     */
    private List<String> columns;

    /**
     * Description of the measurement technique.
     * Maps to {@code schema:measurementTechnique}.
     */
    @JsonProperty("measurement_technique")
    @JsonAlias("measurementTechnique")
    private String measurementTechnique;

    /**
     * 5G/6G testbed context following SNS-JU CMT V1.0.
     * Maps to a {@code dali:testbedContext} blank node.
     */
    @JsonProperty("testbed_context")
    @JsonAlias("testbedContext")
    private TestbedContext testbedContext;

    // ── Legacy / additional fields ────────────────────────────────────────────

    @JsonProperty("record_count")
    @JsonAlias("recordCount")
    private String recordCount;

    @JsonProperty("file_format")
    @JsonAlias("fileFormat")
    private String fileFormat;

    @JsonProperty("number_of_files")
    @JsonAlias("numberOfFiles")
    private Integer number_of_files;

    // ── Custom getters ────────────────────────────────────────────────────────

    public String getTitle() {
        return title != null ? title : datasetId;
    }

    public String getDescription() {
        return description != null ? description : getTitle();
    }

    /** Returns the explicit identifier if set, otherwise falls back to {@code datasetId}. */
    public String getIdentifier() {
        return (identifier != null && !identifier.isEmpty()) ? identifier : datasetId;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords != null ? keywords : new ArrayList<>();
    }

    public void setContributors(List<String> contributors) {
        this.contributors = contributors != null ? contributors : new ArrayList<>();
    }

    public void setRelatedPublications(List<String> relatedPublications) {
        this.relatedPublications = relatedPublications != null ? relatedPublications : new ArrayList<>();
    }
}