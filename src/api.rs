use crate::metrics::REGISTRY;
use prometheus::{Encoder, TextEncoder};
use warp::{Filter, Rejection, Reply};
use wavesexchange_log::{error, info};

pub struct Config {
    pub port: u16,
}

pub async fn start(port: u16) {
    let metrics = warp::path!("metrics").and_then(metrics_handler);

    info!("Starting web server at 0.0.0.0:{}", port);
    warp::serve(metrics).run(([0, 0, 0, 0], port)).await;
}

async fn metrics_handler() -> Result<impl Reply, Rejection> {
    let encoder = TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        error!("could not encode custom metrics: {}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        error!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    res.push_str(&res_custom);
    Ok(res)
}
