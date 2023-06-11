#[cfg(all(unix, feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod impement;

pub mod error;

pub use impement::*;

#[global_allocator]
static ALLOC: impement::Allocator = impement::allocator();
