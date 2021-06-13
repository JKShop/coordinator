use actix_web::web::Json;
use actix_web::{web, App, HttpServer};
use once_cell::sync::Lazy;
use serde::Serialize;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

type WORKER = u16;
type TIMESTAMP = u64;
const REREG_TIME: u64 = 10;

#[derive(PartialEq, Debug, Copy, Clone)]
enum STATUS {
    UNUSED,
    INUSE,
}

#[derive(Serialize)]
struct Response {
    id: WORKER,
    ts: TIMESTAMP,
    re_ts: TIMESTAMP,
}

static WORKER_STATUS: Lazy<Mutex<Vec<(WORKER, STATUS, TIMESTAMP)>>> =
    Lazy::new(|| Mutex::new(Vec::with_capacity(u16::MAX as usize)));

async fn get_id() -> Result<Json<Response>, ()> {
    let mut worker_status = WORKER_STATUS.lock().unwrap();
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    //Get outdated
    let outdated = {
        worker_status
            .iter()
            .filter(|f| f.2 + REREG_TIME < time && f.2 != 0)
            .map(|m| (m.0.clone(), m.1.clone(), m.2.clone()))
            .collect::<Vec<(WORKER, STATUS, TIMESTAMP)>>()
    };

    for od in outdated {
        let idx = { worker_status.iter().position(|f| f == &od).unwrap() };
        worker_status[idx] = (od.0, STATUS::UNUSED, 0);
    }

    let id = {
        worker_status
            .iter()
            .filter(|f| f.1 == STATUS::UNUSED)
            .collect::<Vec<&(WORKER, STATUS, TIMESTAMP)>>()
            .pop()
            .copied()
    };
    match id {
        None => Err(()),
        Some(idv) => {
            let idx = worker_status.iter().position(|f| f == &idv).unwrap();

            worker_status[idx] = (idv.0, STATUS::INUSE, time);
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
    {
        let mut worker_status = WORKER_STATUS.lock().unwrap();
        for worker_id in u16::MIN..u16::MAX {
            (*worker_status).push((worker_id, STATUS::UNUSED, 0));
        }
    }

    let server = HttpServer::new(move || {
        let app = App::new();
        app.service(web::resource("/").route(web::get().to(get_id)))
    })
    .bind("0.0.0.0:8541")?
    .run();
    server.await
}
