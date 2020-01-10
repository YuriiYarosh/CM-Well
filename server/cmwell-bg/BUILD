load("@io_bazel_rules_scala//scala:scala.bzl", "scala_library", "scala_binary", "scala_test")
load("//deps.bzl", "scala_version_short")

lib_dependencies = [
    "@maven//:com_typesafe_config_{}" % scala_version_short,
    "@maven//:com_typesafe_scala-logging_scala-logging_{}" % scala_version_short,

    "@maven//:com_typesafe_akka_akka-stream_{}" % scala_version_short, 
    "@maven//:com_typesafe_akka_akka-stream-contrib_{}" % scala_version_short,
    "@maven//:com_typesafe_akka_akka-agent_{}" % scala_version_short,
    "@maven//:com_typesafe_akka_akka-slf4j_{}" % scala_version_short,
    "@maven//:com_typesafe_akka_akka-stream-kafka_{}" % scala_version_short,

    "@maven//:ch_qos_logback_logback-classic",
    "@maven//:org_slf4j_log4j-over-slf4j",
    
    "@maven//:org_lz4_lz4-java",
    "@maven//:org_elasticsearch_client_transport",
    "@maven//:nl_grons_metrics4-scala",
    "@maven//:nl_grons_metrics4-akka_a25",
    "@maven//:nl_grons_metrics4-scala-hdr",
    "@maven//:org_elasticsearch_metrics-elasticsearch-reporter",
    "@maven//:uk_org_lidalia_sysout-over-slf4j",
]

test_dependencies = [
    "@maven//:net_lingala_zip4j_zip4j",
    "@maven//:com_typesafe_akka_akka-stream-testkit",
] + lib_dependencies
