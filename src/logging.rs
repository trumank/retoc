use std::sync::{Arc, Mutex};

// log macros to check if log and log channel is enabled before performing potentially expensive string formatting
macro_rules! log {
    ($log:expr, $($arg:tt)*) => {
        $log.log(&format!($($arg)*));
    };
}
macro_rules! verbose {
    ($log:expr, $($arg:tt)*) => {
        if $log.verbose_enabled() {
            $log.log(&format!($($arg)*));
        }
    };
}
macro_rules! debug {
    ($log:expr, $($arg:tt)*) => {
        if $log.debug_enabled() {
            $log.log(&format!($($arg)*));
        }
    };
}

pub(crate) use debug;
pub(crate) use log;
pub(crate) use verbose;

pub(crate) struct Log {
    verbose: bool,
    debug: bool,
    progress: Arc<Mutex<Option<indicatif::ProgressBar>>>,
}
impl Log {
    pub(crate) fn new(verbose: bool, debug: bool) -> Self {
        Self {
            verbose,
            debug,
            progress: Default::default(),
        }
    }
    pub(crate) fn set_progress(&self, progress: Option<&indicatif::ProgressBar>) {
        *self.progress.lock().unwrap() = progress.cloned();
    }
    pub(crate) fn allow_stdout(&self) -> bool {
        true
    }
    pub(crate) fn log(&self, msg: &str) {
        if let Some(progress) = self.progress.lock().unwrap().as_ref() {
            progress.println(msg);
        } else {
            println!("{msg}");
        }
    }
    pub(crate) fn verbose_enabled(&self) -> bool {
        self.verbose
    }
    pub(crate) fn debug_enabled(&self) -> bool {
        self.debug
    }
}
