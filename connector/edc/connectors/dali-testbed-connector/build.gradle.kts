plugins {
    `java-library`
    id("application")
    alias(libs.plugins.shadow)
}

dependencies {
    // Common extensions module
    implementation(project(":edc:extensions:transfer-recovery"))
    implementation(project(":edc:extensions:local-files-datasource"))
    implementation(project(":edc:extensions:minio-files-datasource"))
    implementation(project(":edc:extensions:minio-asset-transfer"))
    implementation(project(":edc:extensions:http-data-sink"))
    implementation(project(":edc:extensions:catalog-ui"))
    implementation(project(":edc:extensions:health-endpoint"))
    implementation(project(":edc:extensions:piveau-data-sink"))

    implementation(libs.edc.runtime.core)
    implementation(libs.edc.connector.core)
    implementation(libs.edc.control.api.configuration)
    implementation(libs.edc.control.plane.api.client)
    implementation(libs.edc.control.plane.api)
    implementation(libs.edc.control.plane.core)
    implementation(libs.edc.token.core)
    implementation(libs.edc.dsp)
    implementation(libs.edc.http)
    implementation(libs.edc.configuration.filesystem)
    implementation(libs.edc.iam.mock)
    implementation(libs.edc.management.api)
    // API-key auth for the management API: set web.http.management.auth.type=tokenbased and
    // web.http.management.auth.key (or the env vars WEB_HTTP_MANAGEMENT_AUTH_TYPE / _KEY).
    // auth-tokenbased only provides the "tokenbased" check; auth-configuration is what reads those settings and
    // applies it to the management context. Without it the settings are ignored and the API stays open.
    implementation(libs.edc.auth.tokenbased)
    implementation(libs.edc.auth.configuration)
    implementation(libs.edc.transfer.data.plane.signaling)
    implementation(libs.edc.validator.data.address.http.data)

    implementation(libs.edc.edr.cache.api)
    implementation(libs.edc.edr.store.core)
    implementation(libs.edc.edr.store.receiver)

    implementation(libs.edc.data.plane.selector.api)
    implementation(libs.edc.data.plane.selector.core)

    implementation(libs.edc.data.plane.self.registration)
    implementation(libs.edc.data.plane.signaling.api)
    implementation(libs.edc.data.plane.core)
    implementation(libs.edc.data.plane.http)
    implementation(libs.edc.data.plane.iam)

    // ========================================
    // SQL Store Extensions
    // ========================================
    implementation(libs.edc.sql.core)
    implementation(libs.edc.sql.pool.apache.commons)
    implementation(libs.edc.transaction.local)
    // Asset store
    implementation(libs.edc.asset.index.sql)
    // Contract definition store
    implementation(libs.edc.contract.definition.store.sql)
    // Contract negotiation store
    implementation(libs.edc.contract.negotiation.store.sql)
    // Transfer process store
    implementation(libs.edc.transfer.process.store.sql)
    // Policy store
    implementation(libs.edc.policy.definition.store.sql)
    // Policy monitor store (optional)
    implementation(libs.edc.policy.monitor.store.sql)

    // ========================================
    // MariaDB JDBC Driver
    // ========================================
    implementation("org.postgresql:postgresql:42.7.3")
}

application {
    mainClass.set("org.eclipse.edc.boot.system.runtime.BaseRuntime")
}

var distTar = tasks.getByName("distTar")
var distZip = tasks.getByName("distZip")

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    mergeServiceFiles()
    archiveFileName.set("connector.jar")
    dependsOn(distTar, distZip)
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}
