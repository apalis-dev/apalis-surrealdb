//! Module for various query implementations for the SurrealDb backend.
//!
//! Each submodule contains specific query logic used by the backend.

/// Keep workers alive by updating their heartbeat
pub mod keep_alive;
/// Re-enqueue orphaned tasks that were being processed by dead workers
pub mod reenqueue_orphaned;
/// Register a new worker in the database
pub mod register_worker;
