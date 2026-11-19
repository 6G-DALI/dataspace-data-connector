plugins {
    `java-library`
}

dependencies {
    implementation(libs.edc.control.plane.spi)
    implementation(libs.edc.transfer.spi)
    implementation("org.eclipse.edc:asset-spi:${libs.versions.edc.get()}")
    implementation(libs.edc.data.plane.spi)
    implementation(libs.edc.data.plane.core)
    implementation(libs.edc.data.plane.selector.core)
    implementation(libs.edc.runtime.metamodel)
    implementation(libs.edc.web.spi)

    implementation(libs.minio.io)
}