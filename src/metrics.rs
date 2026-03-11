use super::*;
use crate::error::Result;
use bytes::Bytes;
use error::Error;
use http_body_util::Full;
use hyper::{server::conn::http1, service::service_fn, Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use log::debug;
use prometheus::{register_counter_vec, register_histogram_vec};
use prometheus::{CounterVec, HistogramVec, Opts};
use prometheus::{Encoder, TextEncoder};
use std::convert::Infallible;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[allow(dead_code)]
pub struct Sender {
    server: String,
    sender: mpsc::Sender<InternalMessage>,
}

impl Sender {
    #[allow(dead_code)]
    pub async fn send(&mut self, message: Message) -> Result<()> {
        let server = self.server.clone();
        match message {
            Message::JoinSuccess(t) => {
                self.sender
                    .send(InternalMessage::JoinSuccess(server, t))
                    .await
            }
            Message::JoinFail => self.sender.send(InternalMessage::JoinFail(server)).await,
            Message::DataSuccess(t) => {
                self.sender
                    .send(InternalMessage::DataSuccess(server, t))
                    .await
            }
            Message::DataFail => self.sender.send(InternalMessage::DataFail(server)).await,
        }
        .map_err(|_| Error::MetricsChannel)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum Message {
    JoinSuccess(i64),
    JoinFail,
    DataSuccess(i64),
    DataFail,
}

pub struct Metrics {
    sender: mpsc::Sender<InternalMessage>,
    rx: mpsc::Receiver<InternalMessage>,
    internal_metrics: InternalMetrics,
}

#[derive(Debug)]
#[allow(dead_code)]
enum InternalMessage {
    JoinSuccess(String, i64),
    JoinFail(String),
    DataSuccess(String, i64),
    DataFail(String),
}

struct InternalMetrics {
    join_success_counter: CounterVec,
    join_fail_counter: CounterVec,
    data_success_counter: CounterVec,
    data_fail_counter: CounterVec,
    join_latency: HistogramVec,
    data_latency: HistogramVec,
}

impl Metrics {
    pub fn new(servers: Vec<&String>) -> Metrics {
        let (sender, rx) = mpsc::channel(1024);
        let metrics = InternalMetrics {
            join_success_counter: CounterVec::new(
                Opts::new("join_success", "join success counter"),
                &["server"],
            )
            .unwrap(),
            join_fail_counter: register_counter_vec!("join_fail", "join fail counter", &["server"])
                .unwrap(),
            data_success_counter: register_counter_vec!(
                "data_success",
                "data success counter",
                &["server"],
            )
            .unwrap(),
            data_fail_counter: register_counter_vec!("data_fail", "data fail counter", &["server"])
                .unwrap(),
            join_latency: register_histogram_vec!(
                "join_latency",
                "join latency histogram",
                &["server"],
                vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5],
            )
            .unwrap(),
            data_latency: register_histogram_vec!(
                "data_latency",
                "data latency histogram",
                &["server"],
                vec![0.01, 0.05, 0.1, 0.20, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
            )
            .unwrap(),
        };

        // initialize the counters with 0 so they show up in the HTTP scrape
        for server in servers {
            metrics
                .join_success_counter
                .with_label_values(&[server])
                .reset();
            metrics
                .join_fail_counter
                .with_label_values(&[server])
                .reset();
            metrics
                .data_success_counter
                .with_label_values(&[server])
                .reset();
            metrics
                .data_fail_counter
                .with_label_values(&[server])
                .reset();
        }
        Metrics {
            sender,
            rx,
            internal_metrics: metrics,
        }
    }
    pub async fn run(mut self, shutdown: triggered::Listener, addr: SocketAddr) -> Result {
        // Start Prom Metrics Endpoint
        info!("Prometheus Server listening on http://{}", addr);

        // Bind to the port and listen for incoming TCP connections
        let listener = TcpListener::bind(addr).await?;
        loop {
            tokio::select!(
                _ = shutdown.clone() => {
                    return Ok(())
                }
                result = listener.accept() => {
                    match result {
                        Ok((tcp, addr)) => {
                            info!("Receiving connection from {addr:?}");
                             let io = TokioIo::new(tcp);
                            if let Err(e) = http1::Builder::new()
                                .timer(TokioTimer::new())
                                .serve_connection(io, service_fn(Self::serve_req))
                            .await {
                                error!("Error serving connection: {:?}", e);
                            }
                        }
                        Err(e) => {
                            error!("Error accepting connection: {:?}", e);
                        }
                    }
                }
                msg = self.rx.recv() => {
                    if let Some(msg) = msg {
                        self.handle_msg(msg).await;
                    }
                }
            );
        }
    }

    pub fn get_server_sender(&self, server: &str) -> Sender {
        Sender {
            server: server.to_string(),
            sender: self.sender.clone(),
        }
    }

    pub async fn serve_req(
        _: Request<impl hyper::body::Body>,
    ) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
        let encoder = TextEncoder::new();

        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        let mut buffer_print = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        encoder.encode(&metric_families, &mut buffer_print).unwrap();

        // Output current stats
        debug!("{}", String::from_utf8(buffer_print).unwrap());
        Ok(Response::new(Full::new(Bytes::from(buffer))))
    }

    async fn handle_msg(&mut self, msg: InternalMessage) {
        match msg {
            InternalMessage::JoinSuccess(label, t) => {
                let in_secs = (t as f64) / 1000000.0;
                self.internal_metrics
                    .join_latency
                    .with_label_values(&[&label])
                    .observe(in_secs);
                self.internal_metrics
                    .join_success_counter
                    .with_label_values(&[&label])
                    .inc();
            }
            InternalMessage::JoinFail(label) => self
                .internal_metrics
                .join_fail_counter
                .with_label_values(&[&label])
                .inc(),

            InternalMessage::DataSuccess(label, t) => {
                let in_secs = (t as f64) / 1000000.0;
                self.internal_metrics
                    .data_latency
                    .with_label_values(&[&label])
                    .observe(in_secs);
                self.internal_metrics
                    .data_success_counter
                    .with_label_values(&[&label])
                    .inc();
            }
            InternalMessage::DataFail(label) => self
                .internal_metrics
                .data_fail_counter
                .with_label_values(&[&label])
                .inc(),
        }
    }
}
