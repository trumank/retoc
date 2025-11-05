use std::sync::{Arc, Mutex};
use strum::Display;

// log macros to check if log and log channel is enabled before performing potentially expensive string formatting
#[macro_export]
macro_rules! info {
    ($log:expr, $($arg:tt)*) => {
        if $log.is_level_enabled($crate::logging::LogLevel::Info) {
            $log.log($crate::logging::LogLevel::Info, &format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! verbose {
    ($log:expr, $($arg:tt)*) => {
        if $log.is_level_enabled($crate::logging::LogLevel::Verbose) {
            $log.log($crate::logging::LogLevel::Verbose, &format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! debug {
    ($log:expr, $($arg:tt)*) => {
        if $log.is_level_enabled($crate::logging::LogLevel::Debug) {
            $log.log($crate::logging::LogLevel::Debug, &format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! warning {
    ($log:expr, $($arg:tt)*) => {
        if $log.is_level_enabled($crate::logging::LogLevel::Warning) {
            $log.log($crate::logging::LogLevel::Warning, &format!($($arg)*));
        }
    };
}
#[allow(unused)]
#[macro_export]
macro_rules! error {
    ($log:expr, $($arg:tt)*) => {
        if $log.is_level_enabled($crate::logging::LogLevel::Error) {
            $log.log($crate::logging::LogLevel::Error, &format!($($arg)*));
        }
    };
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Display)]
pub enum LogLevel {
    #[strum(to_string = "debug: ")]
    Debug,
    #[strum(to_string = "verbose: ")]
    Verbose,
    #[strum(to_string = "info: ")]
    Info,
    #[strum(to_string = "warning: ")]
    Warning,
    #[strum(to_string = "error: ")]
    Error,
}

pub trait LogBackend {
    /// Writes messages to this log backend. Note that this function can be called from multiple threads simultaneously
    fn write_message(&self, level: LogLevel, msg: &str);
}

pub struct StdoutLogBackend {}
impl LogBackend for StdoutLogBackend {
    fn write_message(&self, level: LogLevel, msg: &str) {
        if level >= LogLevel::Error {
            eprintln!("{}: {}", level, msg);
        } else {
            println!("{}: {}", level, msg);
        }
    }
}

pub struct NoopLogBackend {}
impl LogBackend for NoopLogBackend {
    fn write_message(&self, _level: LogLevel, _msg: &str) {}
}

pub struct Log {
    min_log_level: LogLevel,
    progress: Arc<Mutex<Option<indicatif::ProgressBar>>>,
    backend: Arc<dyn LogBackend + Send + Sync>,
}
impl Log {
    pub fn new(min_log_level: LogLevel, backend: Arc<dyn LogBackend + Send + Sync>) -> Self {
        Self { min_log_level, backend, progress: Default::default() }
    }
    pub fn new_stdout(verbose: bool, debug: bool) -> Self {
        let min_log_level = if debug {
            LogLevel::Debug
        } else if verbose {
            LogLevel::Verbose
        } else {
            LogLevel::Info
        };
        Self::new(min_log_level, Arc::new(StdoutLogBackend {}))
    }
    pub fn no_log() -> Self {
        Self::new(LogLevel::Error, Arc::new(NoopLogBackend {}))
    }
    pub fn set_progress(&self, progress: Option<&indicatif::ProgressBar>) {
        *self.progress.lock().unwrap() = progress.cloned();
    }
    pub fn is_level_enabled(&self, level: LogLevel) -> bool {
        level >= self.min_log_level
    }
    pub fn log(&self, level: LogLevel, msg: &str) {
        if self.is_level_enabled(level) {
            if let Some(progress) = self.progress.lock().unwrap().as_ref() {
                progress.println(msg);
            } else {
                self.backend.write_message(level, msg);
            }
        }
    }
}
