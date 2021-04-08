use lazy_static::lazy_static;
use prometheus::{IntGaugeVec, Opts, Registry};
lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref WATCHLISTS: IntGaugeVec =
        IntGaugeVec::new(Opts::new("Watchlists", "Watchlists metrics"), &["name"])
            .expect("can't create watchlists metrics");
}

pub fn register_metrics() {
    REGISTRY
        .register(Box::new(WATCHLISTS.clone()))
        .expect("can't register watchlists metrics");
}
