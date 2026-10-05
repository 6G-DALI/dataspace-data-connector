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

package net.sparkworks.edc.extensions.sink.piveau;

import com.rabbitmq.client.ConnectionFactory;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

import java.util.concurrent.Executors;

/**
 * Extension that registers the {@code PiveauData} data sink, which writes transferred files to
 * the MinIO / S3-compatible data lake (configured per transfer via the destination DataAddress)
 * and notifies RabbitMQ when a CSV file has been stored. Catalogue registration is done by the
 * s3-asset-monitor, not here.
 */
public class PiveauDataSinkExtension implements ServiceExtension {

    @Setting(value = "RabbitMQ host", required = false)
    private static final String RABBIT_HOST = "edc.rabbitmq.host";

    @Setting(value = "RabbitMQ port", required = false)
    private static final String RABBIT_PORT = "edc.rabbitmq.port";

    @Setting(value = "RabbitMQ username", required = false)
    private static final String RABBIT_USERNAME = "edc.rabbitmq.username";

    @Setting(value = "RabbitMQ password", required = false)
    private static final String RABBIT_PASSWORD = "edc.rabbitmq.password";

    @Setting(value = "RabbitMQ queue name", required = false)
    private static final String RABBIT_QUEUE = "edc.rabbitmq.queue";

    @Setting(value = "Experiment prefix used to build experiment name from directory name", required = false)
    private static final String EXPERIMENT_PREFIX = "edc.experiment.prefix";

    @Override
    public String name() {
        return "Piveau Routing Data Sink";
    }

    @Inject
    private PipelineService pipelineService;

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();
        var executorService = Executors.newFixedThreadPool(10);

        String rabbitHost     = context.getSetting(RABBIT_HOST, null);
        int    rabbitPort     = Integer.parseInt(context.getSetting(RABBIT_PORT, "5672"));
        String rabbitUsername = context.getSetting(RABBIT_USERNAME, "guest");
        String rabbitPassword = context.getSetting(RABBIT_PASSWORD, "guest");
        String rabbitQueue    = context.getSetting(RABBIT_QUEUE, null);

        ConnectionFactory rabbitConnectionFactory = null;
        if (rabbitHost != null && rabbitQueue != null) {
            rabbitConnectionFactory = new ConnectionFactory();
            rabbitConnectionFactory.setHost(rabbitHost);
            rabbitConnectionFactory.setPort(rabbitPort);
            rabbitConnectionFactory.setUsername(rabbitUsername);
            rabbitConnectionFactory.setPassword(rabbitPassword);
            monitor.info("  RabbitMQ: " + rabbitHost + ":" + rabbitPort + " queue=" + rabbitQueue);
        }

        String experimentPrefix   = context.getSetting(EXPERIMENT_PREFIX, "");

        pipelineService.registerFactory(new PiveauDataSinkFactory(monitor, executorService, rabbitConnectionFactory, rabbitQueue, experimentPrefix));

        monitor.info("✓ Piveau Routing Data Sink registered");
        monitor.info("  Type: PiveauData");
        monitor.info("  JSON and CSV files → MinIO bucket (configured per transfer)");
    }
}
