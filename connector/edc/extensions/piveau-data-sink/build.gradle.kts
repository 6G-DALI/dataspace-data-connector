plugins {
    `java-library`
}

dependencies {
    // EDC dependencies needed for the extension
    implementation(libs.edc.control.plane.spi)
    implementation(libs.edc.transfer.spi)
    implementation(libs.edc.data.plane.spi)
    implementation(libs.edc.runtime.metamodel)
    implementation(libs.edc.data.plane.core)

    // HTTP client (used by PiveauApiHandler) and JSON parsing for Piveau API
    implementation(libs.okhttp)
    implementation(libs.jackson.core)
    implementation(libs.jackson.databind)

    // MinIO client for MinIO streaming data source
    implementation(libs.minio.io)

    // RabbitMQ client for post-transfer notifications
    implementation(libs.rabbitmq.amqp.client)

    // Lombok for reducing boilerplate code
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.4")
}
