load("@io_bazel_rules_scala//scala:scala_toolchain.bzl", "scala_toolchain")

package(default_visibility = ["//visibility:public"])

scala_toolchain(
    name = "cm_well_scala_toolchain_impl",
    scalacopts = ["-Ywarn-unused"],
    scalac_jvm_flags = ["-Xmx2G"],
    scala_test_jvm_flags = ["-Xmx2G"],
    enable_code_coverage_aspect = "on",
    unused_dependency_checker_mode = "warn",
)

toolchain(
    name = "cm_well_scala_toolchain",
    toolchain = "cm_well_scala_toolchain_impl",
    toolchain_type = "@io_bazel_rules_scala//scala:toolchain_type",
    visibility = ["//visibility:public"]
)
