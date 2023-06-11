#[cfg(all(unix, feature = "jemalloc"))]
#[path = "jemalloc.rs"]
mod impement;

pub mod error;

#[cfg(all(unix, feature = "jemalloc"))]
#[path = "jemalloc.rs"]
pub use impement::*;

#[cfg(all(unix, feature = "jemalloc"))]
#[global_allocator]
static ALLOC: impement::Allocator = impement::allocator();
