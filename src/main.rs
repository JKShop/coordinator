#![forbid(unsafe_code)]
#![deny(clippy::needless_borrow, clippy::panic, clippy::unwrap_used)]
#![deny(unused_imports)]
#![forbid(missing_docs)]
//! This crate provides an coordinator, who assigns ID's to requesting services
//! and requires them to re-new them every 600 seconds

use actix_web::web::Json;
use actix_web::{web, App, HttpServer};
use dotenv::{dotenv};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::env;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::PathBuf;

type WorkerId = u16;
type Timestamp = u64;
const REREG_TIME: u64 = 600;

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
enum STATUS {
    Unused,
    Inuse,
}
/// Holds response for / request
#[derive(Serialize)]
pub struct Response {
    /// Worker id of requester
    pub id: WorkerId,
    /// Request timestamp
    pub ts: Timestamp,
    /// Last accepted timestamp, before id is given out again
    pub re_ts: Timestamp,
}

#[derive(Debug, PartialEq, Eq)]
struct Worker {
    pub worker_id: WorkerId,
    pub status: STATUS,
    pub timestamp: Timestamp,
}

static WORKER_STATUS: Lazy<Mutex<Vec<Worker>>> =
    Lazy::new(|| Mutex::new(Vec::with_capacity(u16::MAX as usize)));

async fn re_verify(web::Path((id,)): web::Path<(u16,)>) -> Result<Json<Response>, ()> {
    let mut worker_status = WORKER_STATUS
        .lock()
        .expect("Failed to lock WORKER_STATUS mutex");
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    let idx = {
        worker_status
            .iter()
            .position(|f| f.worker_id == id)
            .expect("Existing item does not exist")
    };
    {
        let worker = &worker_status[idx];

        let update_time = worker.timestamp;
        let max_update_time = update_time + REREG_TIME;

        if time > max_update_time {
            log::warn!("Worker {} failed to re-verify in time !", id);
            return get_id().await;
        }
    }

    worker_status[idx] = Worker {
        worker_id: id,
        status: STATUS::Inuse,
        timestamp: time,
    };

    log::info!("Sending re-validation response for {}", idx);
    Ok(web::Json(Response {
        id,
        ts: time,
        re_ts: time + REREG_TIME,
    }))
}

async fn get_id() -> Result<Json<Response>, ()> {
    let mut worker_status = WORKER_STATUS
        .lock()
        .expect("Failed to lock WORKER_STATUS mutex");
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    //Get outdated
    let outdated = {
        worker_status
            .iter()
            .filter(|f| f.timestamp + REREG_TIME < time && f.timestamp != 0)
            .map(|m| Worker {
                worker_id: m.worker_id,
                status: m.status,
                timestamp: m.timestamp,
            })
            .collect::<Vec<Worker>>()
    };

    for od in outdated {
        let idx = {
            worker_status
                .iter()
                .position(|f| f == &od)
                .expect("Existing item does not exist")
        };
        worker_status[idx] = Worker {
            worker_id: od.worker_id,
            status: STATUS::Unused,
            timestamp: 0,
        };
    }

    let id = {
        worker_status
            .iter()
            .filter(|f| f.status == STATUS::Unused)
            .collect::<Vec<&Worker>>()
            .pop()
    };
    match id {
        None => Err(()),
        Some(idv) => {
            let idx = worker_status
                .iter()
                .position(|f| f == idv)
                .expect("Existing item does not exist");

            worker_status[idx] = Worker {
                worker_id: idv.worker_id,
                status: STATUS::Inuse,
                timestamp: time,
            };
            log::info!("Giving out new worker id: {}", idx);
            Ok(web::Json(Response {
                id: idx as u16,
                ts: time,
                re_ts: time + REREG_TIME,
            }))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();
    match dotenv(){
        Ok(_) => {}
        Err(e) => {
            log::warn!("No .env var found !")
        }
    }
    {
        let mut worker_status = WORKER_STATUS
            .lock()
            .expect("Couldn't lock WORKER_STATUS mutex");
        for worker_id in u16::MIN..u16::MAX {
            (*worker_status).push(Worker {
                worker_id,
                status: STATUS::Unused,
                timestamp: 0,
            });
        }
    }
    log::debug!("Created worker status vec");

    let server = HttpServer::new(move || {
        let app = App::new();
        app.service(web::resource("/").route(web::get().to(get_id)))
            .service(web::resource("/reverify/{id}").route(web::get().to(re_verify)))
    })
    .bind(format!(
        "{}:{}",
        env::var("COORDINATOR.ADDR").expect("Env key COORDINATOR.ADDR not set"),
        env::var("COORDINATOR.PORT").expect("Env key COORDINATOR.PORT not set")
    ))?
    .run();
    log::info!("Spawned server");
    server.await
}
