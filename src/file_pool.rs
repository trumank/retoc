use fs_err as fs;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};

pub struct PooledFileHandle {
    file: Option<fs::File>,
    pool: Arc<FilePoolInner>,
}

impl Drop for PooledFileHandle {
    fn drop(&mut self) {
        // return file handle to pool
        let mut state = self.pool.state.lock().unwrap();
        state.available_files.push_back(self.file.take().unwrap());
        self.pool.condvar.notify_one();
    }
}

impl PooledFileHandle {
    pub fn file(&mut self) -> &mut fs::File {
        self.file.as_mut().unwrap()
    }
}

struct PoolState {
    available_files: VecDeque<fs::File>,
    active_count: usize,
}

struct FilePoolInner {
    path: PathBuf,
    state: Mutex<PoolState>,
    max_handles: usize,
    condvar: Condvar,
}

pub struct FilePool {
    inner: Arc<FilePoolInner>,
}

impl FilePool {
    pub fn new<P: Into<PathBuf>>(path: P, max_handles: usize) -> std::io::Result<Self> {
        let path = path.into();
        // open file once to verify we can
        fs::File::open(&path)?;

        Ok(FilePool {
            inner: Arc::new(FilePoolInner {
                path,
                state: Mutex::new(PoolState {
                    available_files: VecDeque::new(),
                    active_count: 0,
                }),
                max_handles,
                condvar: Condvar::new(),
            }),
        })
    }

    pub fn acquire(&self) -> std::io::Result<PooledFileHandle> {
        let mut state = self.inner.state.lock().unwrap();

        loop {
            // grab an available handle if exists
            if let Some(file) = state.available_files.pop_front() {
                return Ok(PooledFileHandle {
                    file: Some(file),
                    pool: self.inner.clone(),
                });
            }

            // open a new handle if max is not reached
            if state.active_count < self.inner.max_handles {
                let file = fs::File::open(&self.inner.path)?;
                state.active_count += 1;
                return Ok(PooledFileHandle {
                    file: Some(file),
                    pool: self.inner.clone(),
                });
            }

            // must wait for an available handle
            state = self.inner.condvar.wait(state).unwrap();
        }
    }
}
