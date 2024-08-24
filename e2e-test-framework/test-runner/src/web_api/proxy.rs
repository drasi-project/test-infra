use axum::{response::IntoResponse, Extension};

use crate::SharedState;

pub(super) async fn bootstrap_data_handler(
    _state: Extension<SharedState>,
) -> impl IntoResponse {
    log::info!("Processing call - bootstrap_data_handler");

}