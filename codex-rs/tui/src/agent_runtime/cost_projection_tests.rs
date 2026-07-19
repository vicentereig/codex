use super::cost_projection::*;
use super::reducer::AgentObservation;
use super::reducer::AgentRuntimeLimits;
use super::reducer::AgentRuntimeState;
use codex_protocol::ThreadId;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;

const ROOT: &str = "00000000-0000-0000-0000-000000000001";
const CHILD: &str = "00000000-0000-0000-0000-000000000002";

fn id(value: &str) -> ThreadId {
    ThreadId::from_string(value).expect("stable test id")
}

fn target(model: &str) -> PricingTarget {
    PricingTarget {
        provider: "openai".to_string(),
        model: model.to_string(),
        service_tier: Some("priority".to_string()),
    }
}

fn usage() -> CumulativeCostUsage {
    CumulativeCostUsage {
        total_tokens: 3_500,
        non_cached_input_tokens: Some(1_000),
        cached_input_tokens: Some(1_000),
        cache_write_input_tokens: Some(500),
        non_reasoning_output_tokens: Some(700),
        reasoning_output_tokens: Some(300),
    }
}

fn quote(version: &str, rates: &[(PricedTokenCategory, u128)]) -> PricingQuote {
    PricingQuote {
        target: target("known-model"),
        currency: "USD".to_string(),
        source: "provider-published".to_string(),
        version: Some(version.to_string()),
        effective_date: Some("2026-07-01".to_string()),
        nanos_per_million_tokens: rates.iter().copied().collect::<BTreeMap<_, _>>(),
    }
}

fn complete_quote(version: &str) -> PricingQuote {
    quote(
        version,
        &[
            (PricedTokenCategory::NonCachedInput, 2_000_000_000),
            (PricedTokenCategory::CachedInput, 500_000_000),
            (PricedTokenCategory::CacheWriteInput, 2_500_000_000),
            (PricedTokenCategory::NonReasoningOutput, 8_000_000_000),
            (PricedTokenCategory::ReasoningOutput, 8_000_000_000),
        ],
    )
}

fn projection(version: &str, usage: &CumulativeCostUsage) -> EstimatedCostProjection {
    project_estimated_cost(target("known-model"), usage, Some(&complete_quote(version)))
}

#[test]
fn complete_usage_yields_a_labelled_versioned_estimate() {
    let projection = projection("pricing-v1", &usage());

    assert_eq!(
        projection,
        EstimatedCostProjection {
            amount_nanos: Some(11_750_000),
            currency: Some("USD".to_string()),
            provenance: Some(PricingProvenance {
                source: "provider-published".to_string(),
                version: Some("pricing-v1".to_string()),
                effective_date: Some("2026-07-01".to_string()),
            }),
            target: target("known-model"),
            covered_categories: PricedTokenCategory::ALL.to_vec(),
            coverage: CostCoverage::Complete,
            usage_total_tokens: 3_500,
        }
    );
}

#[test]
fn overlapping_or_incomplete_categories_cannot_claim_complete_coverage() {
    let mut inconsistent = usage();
    inconsistent.total_tokens = 3_000;

    assert_eq!(
        projection("pricing-v1", &inconsistent).coverage,
        CostCoverage::Partial
    );
}

#[test]
fn missing_rates_or_usage_are_partial_or_unavailable() {
    let partial_quote = quote(
        "pricing-v1",
        &[
            (PricedTokenCategory::NonCachedInput, 2_000_000_000),
            (PricedTokenCategory::NonReasoningOutput, 8_000_000_000),
        ],
    );
    let partial = project_estimated_cost(target("known-model"), &usage(), Some(&partial_quote));
    assert_eq!(partial.coverage, CostCoverage::Partial);
    assert_eq!(partial.amount_nanos, Some(7_600_000));
    assert_eq!(
        partial.covered_categories,
        vec![
            PricedTokenCategory::NonCachedInput,
            PricedTokenCategory::NonReasoningOutput,
        ]
    );

    let mut missing_usage = usage();
    missing_usage.reasoning_output_tokens = None;
    assert_eq!(
        projection("pricing-v1", &missing_usage).coverage,
        CostCoverage::Partial
    );

    let unavailable = project_estimated_cost(
        target("known-model"),
        &usage(),
        Some(&quote("pricing-v1", &[])),
    );
    assert_eq!(unavailable.coverage, CostCoverage::Unavailable);
    assert_eq!(unavailable.amount_nanos, None);
}

#[test]
fn exact_target_matching_and_reducer_reconciliation_preserve_price_history() {
    let unknown = project_estimated_cost(
        target("unknown-model"),
        &usage(),
        Some(&complete_quote("pricing-v1")),
    );
    assert_eq!(unknown.coverage, CostCoverage::Unavailable);
    assert_eq!(unknown.provenance, None);

    let mut state = AgentRuntimeState::new(id(ROOT), AgentRuntimeLimits::default());
    state.reconcile_estimated_cost(
        id(CHILD),
        projection("pricing-v1", &usage()),
        AgentObservation::live(/*at_ms*/ 1),
    );
    let once = state.snapshot();
    state.reconcile_estimated_cost(
        id(CHILD),
        projection("pricing-v1", &usage()),
        AgentObservation::live(/*at_ms*/ 2),
    );
    assert_eq!(state.snapshot(), once);

    let mut higher_usage = usage();
    higher_usage.total_tokens = 4_000;
    higher_usage.non_cached_input_tokens = Some(1_500);
    let higher_v1 = projection("pricing-v1", &higher_usage);
    state.reconcile_estimated_cost(id(CHILD), higher_v1, AgentObservation::live(/*at_ms*/ 3));
    state.reconcile_estimated_cost(
        id(CHILD),
        projection("pricing-v1", &usage()),
        AgentObservation::live(/*at_ms*/ 4),
    );
    let mut newest_usage = higher_usage;
    newest_usage.total_tokens = 4_500;
    state.reconcile_estimated_cost(
        id(CHILD),
        projection("pricing-v2", &newest_usage),
        AgentObservation::live(/*at_ms*/ 5),
    );

    let repriced = state.snapshot();
    assert_eq!(
        (
            repriced.agents[0].estimated_cost.as_ref().map(|cost| (
                cost.usage_total_tokens,
                cost.amount_nanos,
                cost.coverage
            )),
            repriced.agents[0]
                .estimated_cost
                .as_ref()
                .and_then(|cost| cost.provenance.as_ref())
                .and_then(|provenance| provenance.version.as_deref()),
        ),
        (
            Some((4_000, Some(12_750_000), CostCoverage::Partial)),
            Some("pricing-v1")
        )
    );
}
