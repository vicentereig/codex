//! Versioned estimated-cost projection owned independently of any renderer.
//!
//! Product ownership decision: provider/application integrations own acquiring an
//! immutable price quote; this module owns deterministic projection from a
//! cumulative usage snapshot, and `AgentRuntimeState` retains the result.
//! Renderers only read that projection. This keeps pricing-source ownership
//! neutral, prevents render-time I/O, and leaves subscription or custom-provider
//! cost unavailable unless that integration supplies an exact, versioned quote.

use std::collections::BTreeMap;

const TOKENS_PER_MILLION: u128 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PricedTokenCategory {
    NonCachedInput,
    CachedInput,
    CacheWriteInput,
    NonReasoningOutput,
    ReasoningOutput,
}

impl PricedTokenCategory {
    pub(crate) const ALL: [Self; 5] = [
        Self::NonCachedInput,
        Self::CachedInput,
        Self::CacheWriteInput,
        Self::NonReasoningOutput,
        Self::ReasoningOutput,
    ];
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PricingTarget {
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) service_tier: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CumulativeCostUsage {
    pub(crate) total_tokens: u64,
    pub(crate) non_cached_input_tokens: Option<u64>,
    pub(crate) cached_input_tokens: Option<u64>,
    pub(crate) cache_write_input_tokens: Option<u64>,
    pub(crate) non_reasoning_output_tokens: Option<u64>,
    pub(crate) reasoning_output_tokens: Option<u64>,
}

impl CumulativeCostUsage {
    fn tokens(&self, category: PricedTokenCategory) -> Option<u64> {
        match category {
            PricedTokenCategory::NonCachedInput => self.non_cached_input_tokens,
            PricedTokenCategory::CachedInput => self.cached_input_tokens,
            PricedTokenCategory::CacheWriteInput => self.cache_write_input_tokens,
            PricedTokenCategory::NonReasoningOutput => self.non_reasoning_output_tokens,
            PricedTokenCategory::ReasoningOutput => self.reasoning_output_tokens,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PricingQuote {
    pub(crate) target: PricingTarget,
    pub(crate) currency: String,
    pub(crate) source: String,
    pub(crate) version: Option<String>,
    pub(crate) effective_date: Option<String>,
    pub(crate) nanos_per_million_tokens: BTreeMap<PricedTokenCategory, u128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PricingProvenance {
    pub(crate) source: String,
    pub(crate) version: Option<String>,
    pub(crate) effective_date: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CostCoverage {
    Unavailable,
    Partial,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EstimatedCostProjection {
    /// Billionths of one currency unit. Renderers choose sensible display precision.
    pub(crate) amount_nanos: Option<u128>,
    pub(crate) currency: Option<String>,
    pub(crate) provenance: Option<PricingProvenance>,
    pub(crate) target: PricingTarget,
    pub(crate) covered_categories: Vec<PricedTokenCategory>,
    pub(crate) coverage: CostCoverage,
    pub(crate) usage_total_tokens: u64,
}

pub(crate) fn project_estimated_cost(
    target: PricingTarget,
    usage: &CumulativeCostUsage,
    quote: Option<&PricingQuote>,
) -> EstimatedCostProjection {
    let unavailable = || EstimatedCostProjection {
        amount_nanos: None,
        currency: None,
        provenance: None,
        target: target.clone(),
        covered_categories: Vec::new(),
        coverage: CostCoverage::Unavailable,
        usage_total_tokens: usage.total_tokens,
    };
    let Some(quote) = quote else {
        return unavailable();
    };
    if quote.target != target
        || target.provider.trim().is_empty()
        || target.model.trim().is_empty()
        || quote.currency.trim().is_empty()
        || quote.source.trim().is_empty()
        || !has_text(&quote.version) && !has_text(&quote.effective_date)
    {
        return unavailable();
    }

    let mut covered_categories = Vec::new();
    let mut weighted_nanos = 0_u128;
    for category in PricedTokenCategory::ALL {
        let (Some(tokens), Some(rate)) = (
            usage.tokens(category),
            quote.nanos_per_million_tokens.get(&category),
        ) else {
            continue;
        };
        let Some(contribution) = u128::from(tokens).checked_mul(*rate) else {
            return unavailable();
        };
        let Some(total) = weighted_nanos.checked_add(contribution) else {
            return unavailable();
        };
        weighted_nanos = total;
        covered_categories.push(category);
    }
    if covered_categories.is_empty() {
        return unavailable();
    }

    let covered_token_total = PricedTokenCategory::ALL
        .into_iter()
        .filter_map(|category| usage.tokens(category))
        .try_fold(0_u64, u64::checked_add);
    let coverage = if covered_categories.len() == PricedTokenCategory::ALL.len()
        && covered_token_total == Some(usage.total_tokens)
    {
        CostCoverage::Complete
    } else {
        CostCoverage::Partial
    };
    EstimatedCostProjection {
        amount_nanos: Some(weighted_nanos / TOKENS_PER_MILLION),
        currency: Some(quote.currency.clone()),
        provenance: Some(PricingProvenance {
            source: quote.source.clone(),
            version: quote.version.clone(),
            effective_date: quote.effective_date.clone(),
        }),
        target,
        covered_categories,
        coverage,
        usage_total_tokens: usage.total_tokens,
    }
}

fn has_text(value: &Option<String>) -> bool {
    value
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}
