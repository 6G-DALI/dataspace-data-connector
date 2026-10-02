rootProject.name = "connector"

pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        mavenLocal()
    }
}
include(":edc:extensions:transfer-recovery")
include(":edc:extensions:local-files-datasource")
include(":edc:extensions:minio-files-datasource")
include(":edc:extensions:piveau-data-sink")
include(":edc:extensions:http-data-sink")
include(":edc:extensions:minio-data-sink")
include(":edc:extensions:minio-asset-transfer")
include(":edc:extensions:catalog-ui")
include(":edc:extensions:health-endpoint")
include(":edc:connectors:ac3-uc1")
include(":edc:connectors:dali-testbed-connector")
