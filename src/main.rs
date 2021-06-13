#![forbid(unsafe_code)]
#![deny(
    clippy::needless_borrow,
    clippy::panic,
    clippy::unwrap_used,
    clippy::panic
)]
#![deny(unused_imports)]
#![forbid(missing_docs)]
//! This crate provides an coordinator, who assigns ID's to requesting services
//! and requires them to re-new them every 600 seconds

use actix_web::web::Json;
use actix_web::{web, App, HttpServer};
use dotenv::dotenv;
use once_cell::sync::Lazy;
use serde::Serialize;
use std::env;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

type Worker = u16;
type Timestamp = u64;
const REREG_TIME: u64 = 600;

#[derive(PartialEq, Debug, Copy, Clone)]
enum STATUS {
    Unused,
    Inuse,
}
/// Holds response for / request
#[derive(Serialize)]
pub struct Response {
    /// Worker id of requester
    pub id: Worker,
    /// Request timestamp
    pub ts: Timestamp,
    /// Last accepted timestamp, before id is given out again
    pub re_ts: Timestamp,
}

static WORKER_STATUS: Lazy<Mutex<Vec<(Worker, STATUS, Timestamp)>>> =
    Lazy::new(|| Mutex::new(Vec::with_capacity(u16::MAX as usize)));

async fn get_id() -> Result<Json<Response>, ()> {
    let mut worker_status = WORKER_STATUS.lock().expect("Failed to lock WORKER_STATUS mutex");
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    //Get outdated
    let outdated = {
        worker_status
            .iter()
            .filter(|f| f.2 + REREG_TIME < time && f.2 != 0)
            .map(|m| (m.0, m.1, m.2))
            .collect::<Vec<(Worker, STATUS, Timestamp)>>()
    };

    for od in outdated {
        let idx = { worker_status.iter().position(|f| f == &od).expect("Existing item does not exist") };
        worker_status[idx] = (od.0, STATUS::Unused, 0);
    }

    let id = {
        worker_status
            .iter()
            .filter(|f| f.1 == STATUS::Unused)
            .collect::<Vec<&(Worker, STATUS, Timestamp)>>()
            .pop()
            .copied()
    };
    match id {
        None => Err(()),
        Some(idv) => {
            let idx = worker_status.iter().position(|f| f == &idv).expect("Existing item does not exist");

            worker_status[idx] = (idv.0, STATUS::Inuse, time);
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
    dotenv().expect("Couldn't load env variables !");
    {
        let mut worker_status = WORKER_STATUS
            .lock()
            .expect("Couldn't lock WORKER_STATUS mutex");
        for worker_id in u16::MIN..u16::MAX {
            (*worker_status).push((worker_id, STATUS::Unused, 0));
        }
    }

    let server = HttpServer::new(move || {
        let app = App::new();
        app.service(web::resource("/").route(web::get().to(get_id)))
    })
    .bind(format!(
        "{}:{}",
        env::var("COORDINATOR.ADDR").expect("Env key COORDINATOR.ADDR not set"),
        env::var("COORDINATOR.PORT").expect("Env key COORDINATOR.PORT not set")
    ))?
    .run();
    server.await
}
