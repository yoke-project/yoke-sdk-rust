//! The Rust plugin library's harness.

use yoke_plugin::Declaration;

/// What the harness declares.
pub fn declaration() -> Declaration {
    Declaration::default()
}

/// Runs the harness, and is its exit status.
pub async fn serve(getenv: impl Fn(&str) -> Option<String>) -> i32 {
    let _ = getenv;
    1
}
