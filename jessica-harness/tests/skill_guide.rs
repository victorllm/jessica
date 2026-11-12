//! Behaviour tests for skill retrieval rendering and injection.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use jessica_core::types::{Role, SessionId};
use jessica_harness::{
    DEFAULT_SKILL_INJECT_BYTES, Guide, SkillContent, SkillGuide, SkillSource, SkillSourceError,
};

/// Exact injected title, byte-locked by the tests.
const TITLE: &str = "[技能参考 — 以下内容由历史任务自动提炼，仅供参考，不代表当前事实]";

/// One recorded `SkillSource::recall` call.
#[derive(Debug, Clone, PartialEq)]
struct RecallCall {
    /// Forwarded query.
    query: String,
    /// Forwarded limit.
    limit: usize,
    /// Forwarded minimum score.
    min_score: Option<f64>,
}

/// One fixture skill with its source-side relevance score.
struct ScoredSkill {
    /// Normalized relevance score; larger is more relevant.
    score: f64,
    /// Rendered content.
    content: SkillContent,
}

/// Skill source mock that records calls and applies the documented contract.
struct RecordingSource {
    /// Fixture skills, already in source relevance order.
    skills: Vec<ScoredSkill>,
    /// When set, every recall fails with this error.
    error: Option<SkillSourceError>,
    /// Recorded calls.
    calls: Mutex<Vec<RecallCall>>,
}

impl RecordingSource {
    /// Creates a source that returns `skills` after contract filtering.
    fn returning(skills: Vec<ScoredSkill>) -> Arc<Self> {
        Arc::new(Self {
            skills,
            error: None,
            calls: Mutex::new(Vec::new()),
        })
    }

    /// Creates a source that always fails.
    fn failing(message: &str) -> Arc<Self> {
        Arc::new(Self {
            skills: Vec::new(),
            error: Some(SkillSourceError::Backend {
                message: message.to_owned(),
            }),
            calls: Mutex::new(Vec::new()),
        })
    }

    /// Returns the recorded calls.
    fn calls(&self) -> Vec<RecallCall> {
        self.calls
            .lock()
            .expect("recall call mutex should not be poisoned")
            .clone()
    }
}

#[async_trait]
impl SkillSource for RecordingSource {
    async fn recall(
        &self,
        query: &str,
        limit: usize,
        min_score: Option<f64>,
    ) -> Result<Vec<SkillContent>, SkillSourceError> {
        self.calls
            .lock()
            .expect("recall call mutex should not be poisoned")
            .push(RecallCall {
                query: query.to_owned(),
                limit,
                min_score,
            });

        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if limit == 0 {
            return Ok(Vec::new());
        }

        // None and Some(0.0) disable filtering. A positive value is a
        // normalized relevance threshold; the LIKE fallback's fixed sentinel
        // is 0.0, so a positive threshold excludes it.
        let threshold = match min_score {
            None => None,
            Some(0.0) => None,
            Some(value) => Some(value),
        };
        Ok(self
            .skills
            .iter()
            .filter(|entry| threshold.is_none_or(|threshold| entry.score >= threshold))
            .take(limit)
            .map(|entry| entry.content.clone())
            .collect())
    }
}

/// Builds one skill fixture.
fn skill(name: &str, description: &str, instructions: &str) -> SkillContent {
    SkillContent {
        name: name.to_owned(),
        description: description.to_owned(),
        instructions: instructions.to_owned(),
    }
}

/// Wraps a skill with a relevance score.
fn scored(score: f64, content: SkillContent) -> ScoredSkill {
    ScoredSkill { score, content }
}

/// Builds a session identifier.
fn session(value: &str) -> SessionId {
    SessionId::new(value)
}

#[tokio::test]
async fn an_empty_recall_returns_none() {
    let source = RecordingSource::returning(Vec::new());
    let guide = SkillGuide::new(source.clone(), 5, Some(0.25), 2048);

    let rendered = guide
        .render(&session("session-1"), "deploy")
        .await
        .expect("render should be fail-open");

    assert!(rendered.is_none());
    assert_eq!(
        source.calls(),
        vec![RecallCall {
            query: "deploy".to_owned(),
            limit: 5,
            min_score: Some(0.25),
        }]
    );
}

#[tokio::test]
async fn one_skill_is_rendered_as_one_system_message() {
    let source = RecordingSource::returning(vec![scored(
        1.0,
        skill(
            "Deploy service",
            "Deploys a service safely.",
            "1. Build\n2. Deploy",
        ),
    )]);
    let guide = SkillGuide::new(source, 3, None, 2048);

    let message = guide
        .render(&session("session-1"), "deploy")
        .await
        .expect("render should succeed")
        .expect("the skill should render");

    assert_eq!(message.role, Role::System);
    assert_eq!(
        message.content,
        format!("{TITLE}\n## Deploy service\nDeploys a service safely.\n1. Build\n2. Deploy")
    );
}

#[tokio::test]
async fn multiple_skills_keep_source_order_and_use_the_locked_separator() {
    let source = RecordingSource::returning(vec![
        scored(
            0.9,
            skill("zeta", "first description", "first instructions"),
        ),
        scored(
            0.5,
            skill("alpha", "second description", "second instructions"),
        ),
    ]);
    let guide = SkillGuide::new(source, 3, None, 4096);

    let message = guide
        .render(&session("session-1"), "query")
        .await
        .expect("render should succeed")
        .expect("both skills should render");

    assert_eq!(
        message.content,
        format!(
            "{TITLE}\n## zeta\nfirst description\nfirst instructions\
             \n---\n## alpha\nsecond description\nsecond instructions"
        ),
        "the source order must be preserved and skills joined by exactly \\n---\\n"
    );
    assert_eq!(message.content.matches("\n---\n").count(), 1);
    assert_eq!(message.content.matches("## ").count(), 2);
}

#[tokio::test]
async fn limit_and_min_score_are_forwarded_and_change_the_rendered_set() {
    let source = RecordingSource::returning(vec![
        scored(0.9, skill("top", "top description", "top steps")),
        scored(0.4, skill("middle", "middle description", "middle steps")),
        scored(0.1, skill("bottom", "bottom description", "bottom steps")),
    ]);
    let guide = SkillGuide::new(source.clone(), 2, Some(0.3), 4096);

    let message = guide
        .render(&session("session-1"), "query")
        .await
        .expect("render should succeed")
        .expect("two skills should render");

    assert!(message.content.contains("## top"));
    assert!(message.content.contains("## middle"));
    assert!(!message.content.contains("## bottom"));
    assert_eq!(
        source.calls(),
        vec![RecallCall {
            query: "query".to_owned(),
            limit: 2,
            min_score: Some(0.3),
        }]
    );
}

#[tokio::test]
async fn a_zero_min_score_is_forwarded_unchanged_and_disables_filtering() {
    let source = RecordingSource::returning(vec![
        scored(
            0.0,
            skill("like-fallback", "from LIKE", "short query steps"),
        ),
        scored(0.8, skill("bm25", "from bm25", "long query steps")),
    ]);
    let guide = SkillGuide::new(source.clone(), 5, Some(0.0), 4096);

    let message = guide
        .render(&session("session-1"), "q")
        .await
        .expect("render should succeed")
        .expect("zero-min results should render");

    assert!(message.content.contains("## like-fallback"));
    assert!(message.content.contains("## bm25"));
    assert_eq!(
        source.calls()[0].min_score,
        Some(0.0),
        "0.0 must be forwarded as a disabled threshold, not converted to None"
    );
}

#[tokio::test]
async fn a_positive_min_score_excludes_the_like_fallback_sentinel() {
    let source = RecordingSource::returning(vec![
        scored(
            0.0,
            skill("like-fallback", "from LIKE", "short query steps"),
        ),
        scored(0.8, skill("bm25", "from bm25", "long query steps")),
    ]);
    let guide = SkillGuide::new(source.clone(), 5, Some(0.1), 4096);

    let message = guide
        .render(&session("session-1"), "q")
        .await
        .expect("render should succeed")
        .expect("the bm25 result should render");

    assert!(!message.content.contains("## like-fallback"));
    assert!(message.content.contains("## bm25"));
    assert_eq!(source.calls()[0].min_score, Some(0.1));
}

#[tokio::test]
async fn a_budget_equal_to_the_full_message_does_not_truncate() {
    let content = skill("中文技能", "描述", "步骤中文");
    let expected = format!("{TITLE}\n## 中文技能\n描述\n步骤中文");
    let source = RecordingSource::returning(vec![scored(1.0, content)]);
    let guide = SkillGuide::new(source, 3, None, expected.len());

    let message = guide
        .render(&session("session-1"), "query")
        .await
        .expect("render should succeed")
        .expect("the skill should render");

    assert_eq!(message.content, expected);
}

#[tokio::test]
async fn a_budget_one_or_two_bytes_short_truncates_on_a_utf8_boundary() {
    let content = skill("中文技能", "描述", "步骤中文");
    let expected = format!("{TITLE}\n## 中文技能\n描述\n步骤中文");

    for shortfall in [1_usize, 2] {
        let source = RecordingSource::returning(vec![scored(1.0, content.clone())]);
        let budget = expected.len() - shortfall;
        let guide = SkillGuide::new(source, 3, None, budget);

        let message = guide
            .render(&session("session-1"), "query")
            .await
            .expect("render should succeed")
            .expect("the truncated title should still render");

        assert!(message.content.len() <= budget);
        assert!(expected.starts_with(message.content.as_str()));
        assert_eq!(
            message.content.len(),
            expected.len() - 3,
            "cutting 1-2 bytes into the final 3-byte character must drop it whole"
        );
        assert!(message.content.ends_with('中'));
        assert!(!message.content.ends_with('文'));
    }
}

#[tokio::test]
async fn a_zero_byte_budget_returns_none_without_recalling() {
    let source = RecordingSource::returning(vec![scored(
        1.0,
        skill("deploy", "description", "instructions"),
    )]);
    let guide = SkillGuide::new(source.clone(), 3, None, 0);

    let rendered = guide
        .render(&session("session-1"), "query")
        .await
        .expect("render should be fail-open");

    assert!(rendered.is_none());
    assert!(
        source.calls().is_empty(),
        "a zero budget must not call the source"
    );
}

#[tokio::test]
async fn a_source_error_is_fail_open() {
    let source = RecordingSource::failing("database is locked");
    let guide = SkillGuide::new(source.clone(), 3, None, 2048);

    let rendered = guide
        .render(&session("session-1"), "query")
        .await
        .expect("recall failures must not propagate");

    assert!(rendered.is_none());
    assert_eq!(source.calls().len(), 1);
}

#[tokio::test]
async fn the_title_and_recommendation_annotation_are_byte_locked() {
    assert_eq!(
        TITLE,
        "[技能参考 — 以下内容由历史任务自动提炼，仅供参考，不代表当前事实]"
    );

    let source = RecordingSource::returning(vec![scored(
        1.0,
        skill("deploy", "description", "instructions"),
    )]);
    let guide = SkillGuide::new(source, 3, None, 2048);

    let message = guide
        .render(&session("session-1"), "query")
        .await
        .expect("render should succeed")
        .expect("the skill should render");

    assert!(message.content.starts_with(TITLE));
    assert!(message.content.contains("自动提炼"));
    assert!(message.content.contains("仅供参考"));
    assert!(TITLE.contains("不代表当前事实"));
}

#[tokio::test]
async fn the_session_is_not_used_for_filtering() {
    let source = RecordingSource::returning(vec![scored(
        1.0,
        skill("deploy", "description", "instructions"),
    )]);
    let guide = SkillGuide::new(source.clone(), 3, None, 2048);

    let first = guide
        .render(&session("session-a"), "query")
        .await
        .expect("render should succeed");
    let second = guide
        .render(&session("session-b"), "query")
        .await
        .expect("render should succeed");

    assert_eq!(first, second, "skills are workspace-level");
    assert_eq!(source.calls().len(), 2);
    assert_eq!(source.calls()[0], source.calls()[1]);
}

#[tokio::test]
async fn the_guide_is_object_safe() {
    let source = RecordingSource::returning(vec![scored(
        1.0,
        skill("deploy", "description", "instructions"),
    )]);
    let guide: Arc<dyn Guide> = Arc::new(SkillGuide::new(source, 3, None, 2048));

    let message = guide
        .render(&session("session-1"), "query")
        .await
        .expect("dyn Guide should render");

    assert!(message.is_some());
}

#[tokio::test]
async fn the_default_budget_is_2048_bytes() {
    assert_eq!(DEFAULT_SKILL_INJECT_BYTES, 2048);

    let source = RecordingSource::returning(vec![scored(
        1.0,
        skill("large", "description", &"x".repeat(3000)),
    )]);
    let guide = SkillGuide::with_default_budget(source, 3, None);

    let message = guide
        .render(&session("session-1"), "query")
        .await
        .expect("render should succeed")
        .expect("the truncated skill should render");

    assert_eq!(message.content.len(), DEFAULT_SKILL_INJECT_BYTES);
}

#[test]
fn the_guide_has_no_storage_llm_or_clock_access() {
    let source = include_str!("../src/skill_guide.rs");
    for forbidden in [
        "LlmProvider",
        "EventStore",
        "MemoryStore",
        "SkillLog",
        "rusqlite",
        "SystemTime",
        "Instant::now",
        "std::fs",
        "spawn_blocking",
        "jessica_memory",
    ] {
        assert!(
            !source.contains(forbidden),
            "skill_guide.rs must not reference {forbidden}"
        );
    }
}
