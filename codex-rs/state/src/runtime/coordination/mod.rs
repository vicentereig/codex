mod authority;
mod authority_marker;

pub use authority::CoordinationAuthorityStatus;
pub(crate) use authority::initialize_authority;
#[cfg(test)]
pub(crate) use authority_marker::MARKER_FILE_NAME;
pub(crate) use authority_marker::prepare_fresh_after_corruption_marker;
