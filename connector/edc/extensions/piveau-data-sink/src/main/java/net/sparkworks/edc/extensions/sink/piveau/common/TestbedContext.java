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
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * 5G/6G testbed context metadata following the SNS-JU Common Metadata Template (CMT) V1.0.
 * Maps to a {@code dali:TestbedContext} blank node in DCAT-AP Turtle output.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestbedContext {

    // ── CMT Group 3A: Infrastructure ──────────────────────────────────────────

    /** URI of the underlying testbed platform. Maps to {@code dali:underlayPlatform}. */
    @JsonProperty("underlay_platform")
    @JsonAlias("underlayPlatform")
    private String underlayPlatform;

    /** Deployment environment. Values: indoors | urban | rural | mixed. Maps to {@code dali:environment}. */
    private String environment;

    /** Network domain. Values: RAN | Transport | CORE | E2E. Maps to {@code dali:networkDomain}. */
    @JsonProperty("network_domain")
    @JsonAlias("networkDomain")
    private String networkDomain;

    /** 3GPP release for the RAN segment (e.g. "Release 17"). Maps to {@code dali:ran3gppRelease}. */
    @JsonProperty("ran_3gpp_release")
    @JsonAlias("ran3gppRelease")
    private String ran3gppRelease;

    /** NR type. Values: NR-SA | NR-NSA | LTE. Maps to {@code dali:ranNewRadioType}. */
    @JsonProperty("ran_nr_type")
    @JsonAlias({"ranNewRadioType", "ran_new_radio_type"})
    private String ranNewRadioType;

    /** RAN functional split. Values: DU-RU split | No-Split | CU-DU split. Maps to {@code dali:ranSplit}. */
    @JsonProperty("ran_split")
    @JsonAlias("ranSplit")
    private String ranSplit;

    /** Focused RAN technology. Values: O-RAN | JSAC | RIC | No_focus. Maps to {@code dali:ranFocusedTechnology}. */
    @JsonProperty("ran_focused_technology")
    @JsonAlias("ranFocusedTechnology")
    private String ranFocusedTechnology;

    /** Coverage type. Values: Single_Macro | Single_Micro | Multicell_setup. Maps to {@code dali:ranCoverageType}. */
    @JsonProperty("ran_coverage_type")
    @JsonAlias("ranCoverageType")
    private String ranCoverageType;

    /** NR frequency band (e.g. "n78"). Maps to {@code dali:ranFrequencyBand}. */
    /**
     * Repeatable — a testbed can operate on several bands at once; {@code dali:ranFrequencyBand}
     * has no maxCount. A single string is accepted too.
     */
    @JsonProperty("ran_frequency_band")
    @JsonAlias("ranFrequencyBand")
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    private List<String> ranFrequencyBand;

    /** Channel bandwidth in MHz. Maps to {@code dali:ranBandwidthMHz}. */
    @JsonProperty("ran_bandwidth_mhz")
    @JsonAlias("ranBandwidthMHz")
    private Integer ranBandwidthMHz;

    /** Maximum number of end devices. Maps to {@code dali:ranMaxEndDevices}. */
    @JsonProperty("ran_max_end_devices")
    @JsonAlias("ranMaxEndDevices")
    private Integer ranMaxEndDevices;

    /** UE mobility model. Values: static | pedestrian | vehicular | UAV. Maps to {@code dali:ranMobilityModel}. */
    @JsonProperty("ran_mobility_model")
    @JsonAlias("ranMobilityModel")
    private String ranMobilityModel;

    /** 3GPP release for the Core segment. Maps to {@code dali:coreRelease}. */
    @JsonProperty("core_release")
    @JsonAlias("coreRelease")
    private String coreRelease;

    /** Core solution type. Values: OpenSource | Commercial. Maps to {@code dali:coreSolution}. */
    @JsonProperty("core_solution")
    @JsonAlias("coreSolution")
    private String coreSolution;

    /** Transport technology. Values: wired | microwave | fiber_optics | satellite. Maps to {@code dali:transportType}. */
    @JsonProperty("transport_type")
    @JsonAlias("transportType")
    private String transportType;

    /** Compute orchestrator. Values: Kubernetes | OpenStack | OSM | ONAP. Maps to {@code dali:computeOrchestratorType}. */
    @JsonProperty("compute_orchestrator_type")
    @JsonAlias("computeOrchestratorType")
    private String computeOrchestratorType;

    /** Whether GPU resources are used. Maps to {@code dali:computeGpuUse}. */
    @JsonProperty("compute_gpu_use")
    @JsonAlias("computeGpuUse")
    private Boolean computeGpuUse;

    /** Virtualisation technology. Values: KVM | Docker | Bare-metal. Maps to {@code dali:computeVirtualizationType}. */
    @JsonProperty("compute_virtualization_type")
    @JsonAlias("computeVirtualizationType")
    private String computeVirtualizationType;

    /** Infrastructure type (e.g. "private-edge node"). Maps to {@code dali:computeInfrastructureType}. */
    @JsonProperty("compute_infrastructure_type")
    @JsonAlias("computeInfrastructureType")
    private String computeInfrastructureType;

    // ── CMT Group 3B: Service Description ─────────────────────────────────────

    /** Traffic origin. Values: Manual | Application. Maps to {@code dali:trafficOrigin}. */
    @JsonProperty("traffic_origin")
    @JsonAlias("trafficOrigin")
    private String trafficOrigin;

    /** Traffic pattern (e.g. "UL/DL UDP"). Maps to {@code dali:trafficPattern}. */
    @JsonProperty("traffic_pattern")
    @JsonAlias("trafficPattern")
    private String trafficPattern;

    /** Network slice type. Values: slice101 | Multi-slice | No_slicing. Maps to {@code dali:sliceType}. */
    @JsonProperty("slice_type")
    @JsonAlias("sliceType")
    private String sliceType;

    /** Reference plane. Values: control plane | data plane | management plane. Maps to {@code dali:referencePlane}. */
    @JsonProperty("reference_plane")
    @JsonAlias("referencePlane")
    private String referencePlane;

    /** Related vertical (e.g. "Vertical_agnostic", "CAM", "HEALTH"). Maps to {@code dali:relatedVertical}. */
    @JsonProperty("related_vertical")
    @JsonAlias("relatedVertical")
    private String relatedVertical;

    // ── CMT Group 3C: Experimentation Process & Metrics ───────────────────────

    /** Horizontal observation point (per CMT Annex 1). Maps to {@code dali:observationPointHorizontal}. */
    @JsonProperty("observation_point_horizontal")
    @JsonAlias("observationPointHorizontal")
    private String observationPointHorizontal;

    /** Vertical observation point. Values: Radio Level | Network Layer | Application Layer | etc. Maps to {@code dali:observationPointVertical}. */
    @JsonProperty("observation_point_vertical")
    @JsonAlias("observationPointVertical")
    private String observationPointVertical;

    /** 3GPP TS 28.552 measurement families (e.g. "DRB", "RRU"). Maps to {@code dali:measurementFamily}. */
    @JsonProperty("measurement_family")
    @JsonAlias("measurementFamily")
    private List<String> measurementFamily;

    /** Tools used for measurement (e.g. "Prometheus exporter", "tcpdump"). Maps to {@code dali:measurementTool}. */
    @JsonProperty("measurement_tool")
    @JsonAlias("measurementTool")
    private List<String> measurementTool;
}