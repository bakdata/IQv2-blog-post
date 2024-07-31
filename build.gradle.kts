plugins {
    java
    id("io.freefair.lombok") version "8.6"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.bakdata.kafka.example"
version = "1.0.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven(url = "https://packages.confluent.io/maven/")
}

dependencies {
    val kafkaVersion = "3.7.1"
    implementation(group = "org.apache.kafka", name = "kafka-streams", version = kafkaVersion)
    implementation(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)

    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")

    implementation(group = "io.javalin", name = "javalin", version = "6.1.3")

    val confluentVersion = "7.6.0"
    implementation(group = "io.confluent", name = "kafka-streams-avro-serde", version = confluentVersion)

    implementation(group = "info.picocli", name = "picocli", version = "4.7.5")

    val log4jVersion = "2.23.1"
    implementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    implementation(group = "org.slf4j", name = "slf4j-api", version = "2.0.13")

    val junitVersion = "5.10.2"
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.25.3")

    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = "3.6.0") {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
}

configure<JavaPluginExtension> {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks {
    compileJava {
        options.encoding = "UTF-8"
    }
    compileTestJava {
        options.encoding = "UTF-8"
    }
    test {
        useJUnitPlatform()
        maxParallelForks = 1 // Embedded Kafka does not reliably work in parallel since Kafka 3.0
        maxHeapSize = "4G"
    }
}
