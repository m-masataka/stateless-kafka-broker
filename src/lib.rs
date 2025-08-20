#![allow(clippy::async_fn_in_trait)]
pub mod handler;
pub mod server;
pub mod common;
pub mod storage;
pub mod rest;
pub mod traits;

pub use server::server::server_start;