plugins {
    `java-library`
}

dependencies {
    implementation(libs.edc.runtime.metamodel)
    implementation(libs.edc.web.spi)
    implementation(libs.jakarta.rsApi)
}
