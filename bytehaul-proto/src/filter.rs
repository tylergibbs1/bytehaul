//! Glob-based file filtering for ByteHaul transfers.
//!
//! [`FileFilter`] supports `--include` and `--exclude` patterns with rsync-like
//! semantics: when includes are specified, only matching files pass; excludes
//! always take priority over includes.

use std::path::Path;

use globset::{Glob, GlobSet, GlobSetBuilder};

/// Errors that can occur while building a [`FileFilter`].
#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error("invalid glob pattern: {0}")]
    InvalidPattern(#[from] globset::Error),
}

/// A file filter that applies include/exclude glob patterns.
///
/// # Semantics (rsync-style)
///
/// - When `includes` is empty, all files pass (unless excluded).
/// - When `includes` is non-empty, only files matching at least one include
///   pattern pass.
/// - `excludes` always takes priority: a file matching any exclude pattern is
///   rejected regardless of include patterns.
#[derive(Debug)]
pub struct FileFilter {
    includes: Option<GlobSet>,
    excludes: Option<GlobSet>,
}

impl FileFilter {
    /// Build a new filter from include and exclude pattern slices.
    ///
    /// Returns [`FilterError::InvalidPattern`] if any pattern is malformed.
    pub fn new(include: &[String], exclude: &[String]) -> Result<Self, FilterError> {
        let includes = if include.is_empty() {
            None
        } else {
            let mut builder = GlobSetBuilder::new();
            for pat in include {
                builder.add(Glob::new(pat)?);
            }
            Some(builder.build()?)
        };

        let excludes = if exclude.is_empty() {
            None
        } else {
            let mut builder = GlobSetBuilder::new();
            for pat in exclude {
                builder.add(Glob::new(pat)?);
            }
            Some(builder.build()?)
        };

        Ok(Self { includes, excludes })
    }

    /// Create a no-op filter that matches everything.
    pub fn empty() -> Self {
        Self {
            includes: None,
            excludes: None,
        }
    }

    /// Returns `true` if the file at `relative_path` passes this filter.
    pub fn matches(&self, relative_path: &Path) -> bool {
        // Excludes take priority.
        if let Some(ref excludes) = self.excludes {
            if excludes.is_match(relative_path) {
                return false;
            }
        }

        // When includes are specified, the file must match at least one.
        if let Some(ref includes) = self.includes {
            return includes.is_match(relative_path);
        }

        true
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn empty_filter_matches_everything() {
        let f = FileFilter::empty();
        assert!(f.matches(Path::new("anything.rs")));
        assert!(f.matches(Path::new("sub/dir/file.log")));
    }

    #[test]
    fn new_with_no_patterns_matches_everything() {
        let f = FileFilter::new(&[], &[]).unwrap();
        assert!(f.matches(Path::new("foo.txt")));
    }

    #[test]
    fn include_only_passes_matching() {
        let f = FileFilter::new(
            &["*.rs".to_string(), "*.toml".to_string()],
            &[],
        )
        .unwrap();

        assert!(f.matches(Path::new("main.rs")));
        assert!(f.matches(Path::new("Cargo.toml")));
        assert!(!f.matches(Path::new("README.md")));
        assert!(!f.matches(Path::new("image.png")));
    }

    #[test]
    fn exclude_only_rejects_matching() {
        let f = FileFilter::new(
            &[],
            &["*.log".to_string(), "*.tmp".to_string()],
        )
        .unwrap();

        assert!(f.matches(Path::new("main.rs")));
        assert!(!f.matches(Path::new("debug.log")));
        assert!(!f.matches(Path::new("scratch.tmp")));
    }

    #[test]
    fn exclude_takes_priority_over_include() {
        let f = FileFilter::new(
            &["*.rs".to_string()],
            &["generated_*.rs".to_string()],
        )
        .unwrap();

        assert!(f.matches(Path::new("main.rs")));
        assert!(f.matches(Path::new("lib.rs")));
        assert!(!f.matches(Path::new("generated_bindings.rs")));
    }

    #[test]
    fn glob_with_directory_patterns() {
        let f = FileFilter::new(
            &["src/**/*.rs".to_string()],
            &["**/test_*".to_string()],
        )
        .unwrap();

        assert!(f.matches(Path::new("src/main.rs")));
        assert!(f.matches(Path::new("src/deep/nested/mod.rs")));
        assert!(!f.matches(Path::new("src/test_utils.rs")));
        assert!(!f.matches(Path::new("benches/bench.rs")));
    }

    #[test]
    fn invalid_pattern_returns_error() {
        let result = FileFilter::new(&["[invalid".to_string()], &[]);
        assert!(result.is_err());
    }

    #[test]
    fn exclude_everything_with_star() {
        let f = FileFilter::new(&["*.rs".to_string()], &["*".to_string()]).unwrap();

        // Exclude * matches everything, so nothing passes.
        assert!(!f.matches(Path::new("main.rs")));
        assert!(!f.matches(Path::new("lib.rs")));
    }

    #[test]
    fn multiple_include_patterns_are_unioned() {
        let f = FileFilter::new(
            &["*.rs".to_string(), "*.toml".to_string(), "*.md".to_string()],
            &[],
        )
        .unwrap();

        assert!(f.matches(Path::new("main.rs")));
        assert!(f.matches(Path::new("Cargo.toml")));
        assert!(f.matches(Path::new("README.md")));
        assert!(!f.matches(Path::new("photo.jpg")));
    }

    #[test]
    fn multiple_exclude_patterns_are_unioned() {
        let f = FileFilter::new(
            &[],
            &["*.log".to_string(), "*.tmp".to_string(), "*.bak".to_string()],
        )
        .unwrap();

        assert!(f.matches(Path::new("main.rs")));
        assert!(!f.matches(Path::new("app.log")));
        assert!(!f.matches(Path::new("scratch.tmp")));
        assert!(!f.matches(Path::new("old.bak")));
    }
}
