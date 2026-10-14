//! A run that did only part of the job must not exit with zero.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn retoc(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_retoc")).args(args).output().expect("failed to start retoc")
}

fn describe(output: &Output) -> String {
    format!("exit code {:?}\nstdout: {}\nstderr: {}", output.status.code(), String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr))
}

fn assert_success(output: &Output) {
    assert!(output.status.success(), "expected success, got {}", describe(output));
}

/// Fails with an ordinary error code and the given text on stderr - not with a panic.
fn assert_failure(output: &Output, expected_on_stderr: &str) {
    assert_eq!(output.status.code(), Some(1), "expected exit code 1, got {}", describe(output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(expected_on_stderr), "expected {expected_on_stderr:?} on stderr, got {}", describe(output));
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../retoc/tests/UE4.27")
}

/// Copies the cooked UE 4.27 test package into `input` as package `Game/Content/<name>/TestModUI`.
fn add_package(input: &Path, name: &str, with_exports_file: bool) {
    let package_dir = input.join("Game/Content").join(name);
    fs::create_dir_all(&package_dir).unwrap();
    fs::copy(fixtures().join("TestModUI.uasset"), package_dir.join("TestModUI.uasset")).unwrap();
    if with_exports_file {
        fs::copy(fixtures().join("TestModUI.uexp"), package_dir.join("TestModUI.uexp")).unwrap();
    }
}

fn path_str(path: &Path) -> &str {
    path.to_str().unwrap()
}

#[test]
fn to_zen_does_not_skip_an_asset_silently() {
    let root = fresh_dir("retoc-test-exit-code-to-zen");
    let input = root.join("input");
    add_package(&input, "Good", true);
    add_package(&input, "NoExports", false);

    let utoc = root.join("out").join("Test_P.utoc");
    fs::create_dir_all(utoc.parent().unwrap()).unwrap();

    let refused = retoc(&["to-zen", "--version", "UE4_27", path_str(&input), path_str(&utoc)]);
    assert_failure(&refused, "1 of 2 assets cannot be converted");
    assert!(!utoc.exists(), "a refused run must not leave a container behind");

    let allowed = retoc(&["to-zen", "--version", "UE4_27", "--allow-partial", path_str(&input), path_str(&utoc)]);
    assert_success(&allowed);
    assert!(String::from_utf8_lossy(&allowed.stderr).contains("1 of 2 assets cannot be converted"), "the skipped asset is not reported: {}", describe(&allowed));
    assert!(utoc.exists());

    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn to_legacy_reports_a_package_that_failed() {
    let root = fresh_dir("retoc-test-exit-code-to-legacy");

    // The global container the packages resolve their script imports against, packed
    // from the three chunks kept as fixtures.
    let global_dump = root.join("global-dump");
    fs::create_dir_all(global_dump.join("chunks")).unwrap();
    for (fixture, chunk_id) in [("LoaderInitialLoadMeta_1.bin", "000000000000000000000007"), ("LoaderGlobalNames_1.bin", "000000000000000000000008"), ("LoaderGlobalNameHashes_1.bin", "000000000000000000000009")] {
        fs::copy(fixtures().join(fixture), global_dump.join("chunks").join(chunk_id)).unwrap();
    }
    fs::write(global_dump.join("manifest.json"), r#"{"chunk_paths": {}, "version": "PartitionSize", "mount_point": "../../../"}"#).unwrap();
    let paks = root.join("paks");
    fs::create_dir_all(&paks).unwrap();
    assert_success(&retoc(&["pack-raw", path_str(&global_dump), path_str(&paks.join("global.utoc"))]));

    // Two packages, both good
    let input = root.join("input");
    add_package(&input, "Good", true);
    add_package(&input, "Broken", true);
    let utoc = paks.join("Test_P.utoc");
    assert_success(&retoc(&["to-zen", "--version", "UE4_27", path_str(&input), path_str(&utoc)]));

    // Control: while both are intact the conversion back succeeds with zero
    let intact = retoc(&["to-legacy", path_str(&paks), path_str(&root.join("legacy-intact"))]);
    assert_success(&intact);
    assert!(root.join("legacy-intact/Game/Content/Broken/TestModUI.uexp").exists());

    // Cut one of the two package chunks short and pack the container again
    let dump = root.join("dump");
    assert_success(&retoc(&["unpack-raw", path_str(&utoc), path_str(&dump)]));
    let manifest: String = fs::read_to_string(dump.join("manifest.json")).unwrap();
    let broken_chunk_id = manifest
        .lines()
        .find(|line| line.contains("/Broken/TestModUI.uasset"))
        .and_then(|line| line.split('"').nth(1))
        .expect("the manifest does not list the package to break")
        .to_string();
    let broken_chunk = dump.join("chunks").join(&broken_chunk_id);
    let data = fs::read(&broken_chunk).unwrap();
    fs::write(&broken_chunk, &data[..64]).unwrap();
    assert_success(&retoc(&["pack-raw", path_str(&dump), path_str(&utoc)]));

    let partial = retoc(&["to-legacy", path_str(&paks), path_str(&root.join("legacy-partial"))]);
    assert_failure(&partial, "1 of 2 packages failed to convert");
    assert!(root.join("legacy-partial/Game/Content/Good/TestModUI.uexp").exists(), "the package that converted was not written");
    assert!(!root.join("legacy-partial/Game/Content/Broken/TestModUI.uexp").exists());

    let allowed = retoc(&["to-legacy", "--allow-partial", path_str(&paks), path_str(&root.join("legacy-allowed"))]);
    assert_success(&allowed);
    assert!(String::from_utf8_lossy(&allowed.stderr).contains("1 of 2 packages failed to convert"), "the failed package is not reported: {}", describe(&allowed));

    fs::remove_dir_all(&root).unwrap();
}
