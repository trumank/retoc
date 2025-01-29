mod log;
mod resolvers;

use anyhow::Result;
use resolvers::ResolutionCore;
use tracing::info;

proxy_dll::proxy_dll!([d3d9, d3d11, x3daudio1_7], init);

retour::static_detour! {
  pub static EventDrivenCreateExport: unsafe extern "system" fn(*mut FAsyncPackage, i32, *const(), *const()) -> *const ();
  pub static EventDrivenIndexToObject: unsafe extern "system" fn(*mut FAsyncPackage, FPackageIndex, bool, FPackageIndex) -> *const ();
  pub static CheckForCyclesInner: unsafe extern "system" fn(*mut (), *mut (), *mut (), *mut (), *const FEventLoadNodePtr) -> bool;
}

//bool FEventLoadGraph::CheckForCyclesInner(
//    const TMultiMap<FEventLoadNodePtr,
//    FEventLoadNodePtr>& Arcs,
//    TSet<FEventLoadNodePtr>& Visited,
//    TSet<FEventLoadNodePtr>& Stack,
//    const FEventLoadNodePtr& Visit
//)

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct FPackageIndex(i32);

#[derive(Debug)]
#[repr(C)]
struct FEventLoadNodePtr {
    pkg: Option<Box<FAsyncPackage>>,
    pkg_index: FPackageIndex,
    phase: u32,
}

#[repr(C)]
struct FName {
    a: u32,
    b: u32,
}
impl std::fmt::Debug for FName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fname_to_string: FNameToString =
            unsafe { std::mem::transmute(global().fname_to_string.0) };
        let mut s = FString::default();
        unsafe { fname_to_string(&self, &mut s) };
        write!(f, "{:?}", s.get())
    }
}

#[derive(Debug)]
#[repr(C)]
struct FString {
    ptr: *const u16,
    num: u32,
    max: u32,
}
impl Default for FString {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null(),
            num: 0,
            max: 0,
        }
    }
}
impl FString {
    fn get(&self) -> String {
        let slice = unsafe { std::slice::from_raw_parts(self.ptr, self.num as usize) };
        let pos = slice.iter().position(|&c| c == 0).unwrap_or(slice.len());
        String::from_utf16(&slice[..pos]).unwrap()
    }
}

#[derive(Debug)]
#[repr(C)]
struct FAsyncPackage {
    pad1: u64,
    pad2: u64,
    request_id: u32,
    name: FName,
    name_to_load: FName,
}

type FNameToString = unsafe extern "system" fn(&FName, &mut FString);

pub fn init() {
    log::setup_logging().unwrap();

    info!("Init");

    if let Err(err) = hook() {
        info!("Hook failed {err:?}");
    }
}

static mut GLOBAL: Option<ResolutionCore> = None;
fn global() -> &'static ResolutionCore {
    unsafe { GLOBAL.as_ref().unwrap() }
}

fn hook() -> Result<()> {
    let exe = patternsleuth::process::internal::read_image()?;
    let resolution = exe.resolve(resolvers::ResolutionCore::resolver())?;

    info!("{resolution:#x?}");

    unsafe {
        GLOBAL = Some(resolution);
    }

    unsafe {
        if let Ok(addr) = global().hooks.event_driven_create_export.as_ref() {
            EventDrivenCreateExport
                .initialize(std::mem::transmute(addr.0), move |a, b, c, d| {
                    let package = &*a;
                    info!(
                        "{:?} EventDrivenCreateExport {:>3?}, {:?}",
                        std::thread::current().id(),
                        b,
                        package.name
                    );

                    EventDrivenCreateExport.call(a, b, c, d)
                })
                .unwrap();
            EventDrivenCreateExport.enable().unwrap();
        }

        if let Ok(addr) = global().hooks.event_driven_index_to_object.as_ref() {
            EventDrivenIndexToObject
                .initialize(std::mem::transmute(addr.0), move |a, b, c, d| {
                    let package = &*a;

                    info!(
                        "{:?} EventDrivenIndexToObject {:>3?} {:>3?} {:>3?} {:?}",
                        std::thread::current().id(),
                        b,
                        c,
                        d,
                        package.name
                    );

                    EventDrivenIndexToObject.call(a, b, c, d)
                })
                .unwrap();
            EventDrivenIndexToObject.enable().unwrap();
        }

        if let Ok(addr) = global().hooks.check_for_cycles_inner.as_ref() {
            CheckForCyclesInner
                .initialize(std::mem::transmute(addr.0), move |a, b, c, d, e| {
                    let result = CheckForCyclesInner.call(a, b, c, d, e);

                    if let Some(f) = e.as_ref() {
                        let node = &*f;
                        info!(
                            "{:?} CheckForCyclesInner cycle={result} {:?}",
                            std::thread::current().id(),
                            node
                        );
                    }

                    result
                })
                .unwrap();
            CheckForCyclesInner.enable().unwrap();
        }
    }
    Ok(())
}
