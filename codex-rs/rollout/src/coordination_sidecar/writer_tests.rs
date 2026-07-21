use pretty_assertions::assert_eq;
use tempfile::TempDir;

use super::record::DegradationSidecarRecord;
use super::record::NativeSidecarRecord;
use super::record::SidecarRecord;
use super::writer::AppendOutcome;
use super::writer::SidecarFailureInjector;
use super::writer::SidecarWriteError;
use super::writer::SidecarWriter;
use super::writer::WriteStep;

fn native(event_id: &str, revision: u64, materialized_at_ms: i64) -> SidecarRecord {
    SidecarRecord::Native(NativeSidecarRecord {
        event_id: event_id.to_string(),
        root_thread_id: "019f7c6c-1111-7000-8000-000000000601".to_string(),
        state_epoch: "019f7c6c-0000-7000-8000-000000000001".to_string(),
        revision,
        materialized_at_ms,
    })
}

fn degradation(degradation_id: &str, after_revision: u64, source_ordinal: u64) -> SidecarRecord {
    SidecarRecord::Degradation(DegradationSidecarRecord {
        degradation_id: degradation_id.to_string(),
        root_thread_id: "019f7c6c-1111-7000-8000-000000000601".to_string(),
        state_epoch: "019f7c6c-0000-7000-8000-000000000001".to_string(),
        after_revision,
        source_ordinal,
        stable_record_id: degradation_id.to_string(),
        materialized_at_ms: 1_000,
    })
}

struct FailAt(WriteStep);

impl SidecarFailureInjector for FailAt {
    fn before_step(&self, step: WriteStep) -> std::io::Result<()> {
        if step == self.0 {
            return Err(std::io::Error::other(format!(
                "injected failure at {step:?}"
            )));
        }
        Ok(())
    }
}

#[tokio::test]
async fn appends_and_dedupes_exact_identity() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    let mut writer = SidecarWriter::open(path.clone()).await?;

    let record = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);
    assert_eq!(
        writer.append_if_new(&record).await?,
        AppendOutcome::Appended
    );
    // Exact retry of the same record is a no-op, not a duplicate line.
    assert_eq!(
        writer.append_if_new(&record).await?,
        AppendOutcome::AlreadyPresent
    );
    assert_eq!(writer.path(), path.as_path());

    let contents = tokio::fs::read_to_string(&path).await?;
    assert_eq!(contents.lines().count(), 1, "no duplicate line written");
    Ok(())
}

#[tokio::test]
async fn replay_after_retry_ignores_ephemeral_materialized_at() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    let mut writer = SidecarWriter::open(path.clone()).await?;

    let first = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);
    assert_eq!(writer.append_if_new(&first).await?, AppendOutcome::Appended);
    // Same identity, same structural fields, but a different materialization
    // timestamp (as a real retry at a later wall-clock time would produce):
    // must be recognized as the same logical record, not divergent.
    let retried = native("019f7c6c-2222-7000-8000-000000000701", 1, 9_999);
    assert_eq!(
        writer.append_if_new(&retried).await?,
        AppendOutcome::AlreadyPresent
    );
    let contents = tokio::fs::read_to_string(&path).await?;
    assert_eq!(contents.lines().count(), 1);
    Ok(())
}

#[tokio::test]
async fn divergent_identity_fails_closed() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    let mut writer = SidecarWriter::open(path.clone()).await?;

    let first = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);
    writer.append_if_new(&first).await?;

    // Same identity (event_id), different revision: a genuinely divergent
    // record must quarantine, not silently accept or overwrite.
    let divergent = native("019f7c6c-2222-7000-8000-000000000701", 2, 1_000);
    assert!(matches!(
        writer.append_if_new(&divergent).await,
        Err(SidecarWriteError::DivergentIdentity)
    ));
    let contents = tokio::fs::read_to_string(&path).await?;
    assert_eq!(
        contents.lines().count(),
        1,
        "divergent record is never written"
    );
    Ok(())
}

#[tokio::test]
async fn malformed_record_fails_closed_before_any_write() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    let mut writer = SidecarWriter::open(path.clone()).await?;

    let malformed = native("", 1, 1_000);
    assert!(writer.append_if_new(&malformed).await.is_err());
    assert!(
        !path.exists(),
        "no file created for a record that never validated"
    );
    Ok(())
}

#[tokio::test]
async fn two_roots_same_date_prove_path_uniqueness() -> anyhow::Result<()> {
    // Simulates two roots created on the same date: their sidecar paths
    // differ only by the root-rollout-stem prefix, proving the stem (not
    // just the epoch) is what makes the path unique.
    let dir = TempDir::new()?;
    let epoch = "019f7c6c-0000-7000-8000-000000000001";
    let path_a = dir.path().join(format!(
        "rollout-2026-07-21T10-00-00-thread-a.coordination-v1-{epoch}.jsonl"
    ));
    let path_b = dir.path().join(format!(
        "rollout-2026-07-21T10-00-00-thread-b.coordination-v1-{epoch}.jsonl"
    ));
    assert_ne!(path_a, path_b);

    let mut writer_a = SidecarWriter::open(path_a.clone()).await?;
    let mut writer_b = SidecarWriter::open(path_b.clone()).await?;
    writer_a
        .append_if_new(&native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000))
        .await?;
    writer_b
        .append_if_new(&native("019f7c6c-3333-7000-8000-000000000801", 1, 1_000))
        .await?;

    let a_contents = tokio::fs::read_to_string(&path_a).await?;
    let b_contents = tokio::fs::read_to_string(&path_b).await?;
    assert!(a_contents.contains("019f7c6c-2222-7000-8000-000000000701"));
    assert!(b_contents.contains("019f7c6c-3333-7000-8000-000000000801"));
    assert!(!a_contents.contains("019f7c6c-3333-7000-8000-000000000801"));
    Ok(())
}

/// Exhaustive crash matrix: inject a failure at each named durability seam
/// (create, write, flush, file-sync, directory-sync), then reopen (rescan,
/// exactly as a process restart would) and prove a clean retry converges to
/// exactly one durable, correct line either way.
///
/// `Create`/`Open`/`Write` fire *before* `write_all` runs, so a failure there
/// means no bytes reached the file at all — the reopened retry must append
/// fresh (`Appended`). `Flush`/`FileSync`/`DirSync` fire *after* `write_all`
/// already returned successfully; a real crash there could still lose the
/// write on some filesystems if it landed before the OS's own durability
/// point, but *this test cannot simulate an actual power-loss/OS crash* — it
/// only simulates the caller stopping. Since `write_all` already completed
/// against the real filesystem the test runs on, the bytes are genuinely
/// present when we reopen, so the honest, correct expectation there is
/// `AlreadyPresent`, not a second physical write. Either outcome proves the
/// same property: retrying after any of these seams never produces a
/// duplicate line and always converges to exactly one durable, correct
/// record.
#[tokio::test]
async fn crash_matrix_reopen_after_every_seam_converges() -> anyhow::Result<()> {
    let before_write = [WriteStep::Create, WriteStep::Write];
    let after_write = [WriteStep::Flush, WriteStep::FileSync, WriteStep::DirSync];
    for step in before_write.into_iter().chain(after_write) {
        let dir = TempDir::new()?;
        let path = dir.path().join("root.coordination-v1-epoch.jsonl");
        let record = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);

        let mut writer = SidecarWriter::open(path.clone()).await?;
        let result = writer.append_if_new_with(&record, &FailAt(step)).await;
        assert!(result.is_err(), "{step:?} should abort the append");

        // Reopen (rescan) exactly as a process restart would, then retry.
        let mut reopened = SidecarWriter::open(path.clone()).await?;
        let expected = if before_write.contains(&step) {
            AppendOutcome::Appended
        } else {
            AppendOutcome::AlreadyPresent
        };
        assert_eq!(
            reopened.append_if_new(&record).await?,
            expected,
            "{step:?}: retry after crash must converge without duplicating"
        );
        // A second retry (as if the ack itself then also had to be redone)
        // must always be a pure no-op.
        assert_eq!(
            reopened.append_if_new(&record).await?,
            AppendOutcome::AlreadyPresent,
            "{step:?}: second retry must not duplicate"
        );

        let contents = tokio::fs::read_to_string(&path).await?;
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(
            lines.len(),
            1,
            "{step:?}: exactly one durable line survives"
        );
        let parsed: SidecarRecord = serde_json::from_str(lines[0])?;
        assert_eq!(parsed, record, "{step:?}: whole-record equality on replay");
    }
    Ok(())
}

#[tokio::test]
async fn crash_between_open_and_write_on_existing_file_still_converges() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    let first = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);
    let mut writer = SidecarWriter::open(path.clone()).await?;
    writer.append_if_new(&first).await?;

    // A second, distinct record hits the "Open" (not "Create") seam because
    // the file and its directory entry already exist.
    let second = native("019f7c6c-2222-7000-8000-000000000702", 2, 1_100);
    let result = writer
        .append_if_new_with(&second, &FailAt(WriteStep::Open))
        .await;
    assert!(result.is_err());

    let mut reopened = SidecarWriter::open(path.clone()).await?;
    assert_eq!(
        reopened.append_if_new(&second).await?,
        AppendOutcome::Appended
    );
    let contents = tokio::fs::read_to_string(&path).await?;
    assert_eq!(contents.lines().count(), 2);
    Ok(())
}

#[tokio::test]
async fn torn_tail_from_interrupted_first_append_is_recovered() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    // Simulate a crash mid-write: a file with a truncated, unparseable, not
    // newline-terminated JSON fragment.
    tokio::fs::write(&path, b"{\"kind\":\"native\",\"eventId\":\"01").await?;

    let mut writer = SidecarWriter::open(path.clone()).await?;
    let record = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);
    assert_eq!(
        writer.append_if_new(&record).await?,
        AppendOutcome::Appended
    );
    let contents = tokio::fs::read_to_string(&path).await?;
    let lines: Vec<&str> = contents.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "torn tail truncated, exactly one clean line remains"
    );
    Ok(())
}

#[tokio::test]
async fn torn_tail_with_prior_good_line_preserves_it() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    let mut writer = SidecarWriter::open(path.clone()).await?;
    let first = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);
    writer.append_if_new(&first).await?;
    // Append a torn tail by hand after the first good line.
    let mut bytes = tokio::fs::read(&path).await?;
    bytes.extend_from_slice(b"{\"kind\":\"native\",\"eventId\":\"broken");
    tokio::fs::write(&path, &bytes).await?;

    let mut reopened = SidecarWriter::open(path.clone()).await?;
    let contents = tokio::fs::read_to_string(&path).await?;
    assert_eq!(contents.lines().count(), 1, "torn tail truncated away");
    assert!(contents.contains("019f7c6c-2222-7000-8000-000000000701"));

    // The first (already-durable) record still dedupes correctly.
    assert_eq!(
        reopened.append_if_new(&first).await?,
        AppendOutcome::AlreadyPresent
    );
    Ok(())
}

#[tokio::test]
async fn corrupt_non_tail_line_fails_closed_at_open() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    // A malformed line followed by a well-formed one: the malformed line is
    // NOT the tail, so this is genuine corruption, not a torn write.
    tokio::fs::write(&path, b"not json at all\n{\"kind\":\"native\"}\n").await?;

    assert!(matches!(
        SidecarWriter::open(path).await,
        Err(SidecarWriteError::CorruptSidecar)
    ));
    Ok(())
}

#[tokio::test]
async fn degradation_records_dedupe_independently_of_native() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let path = dir.path().join("root.coordination-v1-epoch.jsonl");
    let mut writer = SidecarWriter::open(path.clone()).await?;

    let native_record = native("019f7c6c-2222-7000-8000-000000000701", 1, 1_000);
    let degradation_record = degradation("019f7c6c-4444-7000-8000-000000000901", 0, 0);
    writer.append_if_new(&native_record).await?;
    writer.append_if_new(&degradation_record).await?;
    assert_eq!(
        writer.append_if_new(&degradation_record).await?,
        AppendOutcome::AlreadyPresent
    );
    let contents = tokio::fs::read_to_string(&path).await?;
    assert_eq!(contents.lines().count(), 2);
    Ok(())
}
