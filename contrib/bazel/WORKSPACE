load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("//deps.bzl", "java_deps", "jena_deps")

## Maven Dependencies ##

rules_jvm_external_version = "140a3a24d38913b17051c738218ddc659ce93026"
rules_jvm_external_version_sha256 = "38f81e3d1bf38ede56aed2d34f6c4a2752c53db5506006f6f6e42c92b9538b70"

http_archive(
    name = "rules_jvm_external",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.tar.gz" % rules_jvm_external_version,
    strip_prefix = "rules_jvm_external-%s" % rules_jvm_external_version,
    sha256 = rules_jvm_external_version_sha256,
)

load("@rules_jvm_external//:defs.bzl", "maven_install", "artifact")
load("@rules_jvm_external//:specs.bzl", "maven")

## Scala ##

rules_scala_version="886bc9cf6d299545510b39b4872bbb5dc7526cb3"
rules_scala_version_sha256="b6ecd5ad81be1490408e6714914e5766bed6b25a0ca0c8671473c84bb7b3ae51"

http_archive(
    name = "io_bazel_rules_scala",
    type = "tar.gz",
    url = "https://github.com/bazelbuild/rules_scala/archive/%s.tar.gz" % rules_scala_version,
    strip_prefix = "rules_scala-%s" % rules_scala_version,
    sha256 = rules_scala_version_sha256,
)

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")
scala_register_toolchains("//:cm_well_scala_toolchain")

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")
scala_repositories((
    "2.12.10", {
       "scala_compiler": "cedc3b9c39d215a9a3ffc0cc75a1d784b51e9edc7f13051a1b4ad5ae22cfbc0c",
       "scala_library": "0a57044d10895f8d3dd66ad4286891f607169d948845ac51e17b4c1cf0ab569d",
       "scala_reflect": "56b609e1bab9144fb51525bfa01ccd72028154fc40a58685a1e9adcbe7835730"
    }
))

## Protobuf ##

protobuf_version="fe1790ca0df67173702f70d5646b82f48f412b99"
protobuf_version_sha256="7adbf4833bc56e201db3076e864f6f4fd3043b5895e5f7e6ab953d385b49a926"

http_archive(
    name = "com_google_protobuf",
    type = "tar.gz",
    url = "https://github.com/protocolbuffers/protobuf/archive/%s.tar.gz" % protobuf_version,
    strip_prefix = "protobuf-%s" % protobuf_version,
    sha256 = protobuf_version_sha256,
)

## Skylib ##

skylib_version = "1.0.2"
http_archive(
    name = "bazel_skylib",
    type = "tar.gz",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib-{}.tar.gz".format (skylib_version, skylib_version),
        "https://github.com/bazelbuild/bazel-skylib/releases/download/{}/bazel-skylib-{}.tar.gz".format (skylib_version, skylib_version),
    ],
    sha256 = "97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44",
)
load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")
bazel_skylib_workspace()

## Project dependencies

maven_install(
    artifacts = [
        # maven.artifact(
        #     group = "", artifact = "", version = "",
        #     exclusions = []
        # ),
    ],
    maven_install_json = "//:maven_install.json",
    repositories = [
        "https://maven.google.com",
        "https://jcenter.bintray.com/",
        "https://repo1.maven.org/maven2",
    ],
    fetch_sources = True,
    fail_on_missing_checksum = True,
    version_conflict_policy = "pinned",
    strict_visibility = True
)

load("@maven//:defs.bzl", "pinned_maven_install")
pinned_maven_install()
