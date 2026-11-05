use chrono::Local;
use log::{LevelFilter, Log, Metadata, Record, SetLoggerError};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

struct Logger {
    level: LevelFilter,
    file: Option<Mutex<File>>,
}

impl Logger {
    fn new(level: LevelFilter, log_path: Option<&str>) -> Self {
        let file = log_path.and_then(|path| {
            if let Some(parent) = Path::new(path).parent() {
                if !parent.as_os_str().is_empty() {
                    let _ = std::fs::create_dir_all(parent);
                }
            }

            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()
                .map(Mutex::new)
        });

        Logger { level, file }
    }
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let message = format!(
                "[{}] {} [{}:{}] - {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.module_path().unwrap_or("unknown"),
                record.line().unwrap_or(0),
                record.args()
            );

            println!("{}", message);

            if let Some(ref file_mutex) = self.file {
                if let Ok(mut file) = file_mutex.lock() {
                    let _ = writeln!(file, "{}", message);
                    let _ = file.flush();
                }
            }
        }
    }

    fn flush(&self) {
        if let Some(ref file_mutex) = self.file {
            if let Ok(mut file) = file_mutex.lock() {
                let _ = file.flush();
            }
        }
    }
}

static LOGGER: OnceLock<Logger> = OnceLock::new();


pub fn init_logger(log_path: Option<String>) -> Result<(), SetLoggerError> {
    let level = std::env::var("LOG_LEVEL")
        .ok()
        .and_then(|s| s.parse::<LevelFilter>().ok())
        .unwrap_or(LevelFilter::Info);

    let logger = Logger::new(level, log_path.as_deref());
    LOGGER.set(logger).ok();
    log::set_logger(LOGGER.get().expect("Logger should be initialized"))?;
    log::set_max_level(level);
    Ok(())
}