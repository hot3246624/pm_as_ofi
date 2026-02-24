// ─── V2: Toxicity-Aware StatArb Actor Architecture ───
pub mod coordinator;
pub mod executor;
pub mod inventory;
pub mod messages;
pub mod ofi;
pub mod user_ws;

// ─── Shared types (kept from V1, used by both old and new) ───
pub mod types;

// ─── V1 Legacy (archived for API signing, JSON serialization reference) ───
pub mod legacy;
