//! The Rust interface library's harness, which the conformance suite drives through the socket
//! CONFORMANCE_SOCKET names.

#[tokio::main]
async fn main() {
    std::process::exit(yoke_interface_harness::serve(|k| std::env::var(k).ok()).await);
}
