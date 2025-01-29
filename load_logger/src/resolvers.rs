use patternsleuth::{
    resolvers::{
        ensure_one,
        futures::future::join_all,
        impl_collector, impl_resolver_singleton, impl_try_collector,
        unreal::{engine_version::EngineVersion, fname::FNameToString, util},
    },
    scanner::Pattern,
};

impl_try_collector! {
    #[derive(Debug, PartialEq)]
    pub struct ResolutionCore {
        pub engine_version: EngineVersion,
        pub fname_to_string: FNameToString,
        pub hooks: ResolutionHooks,
    }
}

impl_collector! {
    #[derive(Debug, PartialEq)]
    pub struct ResolutionHooks {
        pub event_driven_create_export: EventDrivenCreateExport,
        pub event_driven_index_to_object: EventDrivenIndexToObject,
        pub check_for_cycles_inner: CheckForCyclesInner,
    }
}

#[derive(Debug, PartialEq)]
pub struct EventDrivenCreateExport(pub usize);
impl_resolver_singleton!(all, EventDrivenCreateExport, |ctx| async {
    let strings = ctx
        .scan(util::utf16_pattern("EventDrivenCreateExport\0"))
        .await;
    let refs = util::scan_xrefs(ctx, &strings).await;
    let fns = util::root_functions(ctx, &refs)?;
    Ok(Self(ensure_one(fns)?))
});

#[derive(Debug, PartialEq)]
pub struct EventDrivenIndexToObject(pub usize);
impl_resolver_singleton!(all, EventDrivenIndexToObject, |ctx| async {
    let strings = ctx
        .scan(util::utf16_pattern(
            "Missing Dependency, request for %s but it was still waiting for creation.\0",
        ))
        .await;
    let refs = util::scan_xrefs(ctx, &strings).await;
    let fns = util::root_functions(ctx, &refs)?;
    Ok(Self(ensure_one(fns)?))
});

#[derive(Debug, PartialEq)]
pub struct CheckForCyclesInner(pub usize);
impl_resolver_singleton!(all, CheckForCyclesInner, |ctx| async {
    let patterns = [
        "4c 89 44 24 18 48 89 4c 24 08 55 56 41 54 41 55 41 57 48 81 ec ?? 00 00 00 4c 8b ac 24 ?? 00 00 00 4d 8b e0 48 8b f2 4d 8b c5 48 8d 54 24 34",
    ];

    let res = join_all(patterns.iter().map(|p| ctx.scan(Pattern::new(p).unwrap()))).await;

    Ok(Self(ensure_one(res.into_iter().flatten())?))
});
