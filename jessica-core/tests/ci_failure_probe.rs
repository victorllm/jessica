#[test]
fn ci_failure_probe() {
    let actual = 1;
    let expected = 2;
    assert_eq!(actual, expected, "intentional CI failure probe; do not merge");
}
