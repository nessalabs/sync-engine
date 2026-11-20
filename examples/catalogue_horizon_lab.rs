//! Owned native fixture; positive horizons are injected only by the Python lab.
use nessa_sync::replication::{
    catalogue::*,
    domain::{CapabilitySupport, Id, Scope},
    infrastructure::{MemoryAuthorizer, SqliteCatalogueSource, SqliteCatalogueStore},
};
use std::{env, error::Error, path::PathBuf, time::Instant};
fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
fn scope() -> Scope {
    Scope::new(
        id("receiver"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
        id("epoch"),
    )
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        return Err("usage: catalogue_horizon_lab OWNED_DIRECTORY init|bootstrap|churn|partial|follow|refuse|inspect".into());
    }
    let dir = PathBuf::from(&args[1]);
    if !dir.is_dir() {
        return Err("fixture directory must already exist".into());
    }
    let mut s = SqliteCatalogueSource::open(
        dir.join("source.db"),
        id("origin"),
        id("stream"),
        id("incarnation"),
        id("schema"),
    )?;
    let mut d = SqliteCatalogueStore::open(dir.join("cache.db"))?;
    let mut a = MemoryAuthorizer::allowed(scope());
    let support = CapabilitySupport::current();
    let start = Instant::now();
    match args[2].as_str() {
        "init" => {
            assert_eq!(
                s.horizon_head(&scope(), support).unwrap().source.position(),
                0
            );
            for n in 0..620 {
                s.upsert(&id(&format!("item-{n:04}")), b"catalogue-value")
                    .unwrap();
            }
        }
        "churn" => {
            for n in 0..160 {
                s.delete(&id(&format!("item-{n:04}"))).unwrap();
            }
        }
        "bootstrap" | "follow" => {
            while let Some(p) =
                begin_or_resume_with_horizon(&scope(), support, &mut a, &mut s, &mut d).unwrap()
            {
                let mut active = Some(p);
                while let Some(p) = active {
                    active = apply_next_page_with_horizon(
                        &p, 64, 1048576, support, &mut a, &mut s, &mut d,
                    )
                    .unwrap()
                    .active;
                }
            }
        }
        "partial" => {
            let p = begin_or_resume_with_horizon(&scope(), support, &mut a, &mut s, &mut d)
                .unwrap()
                .unwrap();
            assert!(
                apply_next_page_with_horizon(&p, 64, 1048576, support, &mut a, &mut s, &mut d)
                    .unwrap()
                    .active
                    .is_some()
            );
        }
        "refuse" => {
            let before = d.progress(&scope()).unwrap();
            assert!(matches!(
                begin_or_resume_with_horizon(&scope(), support, &mut a, &mut s, &mut d),
                Err(CatalogueError::Source(
                    CatalogueSourceError::HorizonExpired { .. }
                ))
            ));
            assert_eq!(d.progress(&scope()).unwrap(), before);
        }
        "inspect" => {}
        _ => return Err("unknown fixture operation".into()),
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    let p = d.progress(&scope()).unwrap();
    let completed = p.as_ref().map_or(0, |p| p.completed);
    let active = p.as_ref().is_some_and(|p| p.active.is_some());
    let mut live = 0;
    let mut deleted = 0;
    for n in 0..620 {
        if let Some(v) = d
            .cached_entry(&scope(), &id(&format!("item-{n:04}")))
            .unwrap()
        {
            assert_eq!(v.manifest.key.id, id(&format!("item-{n:04}")));
            assert_eq!(v.manifest.key.creation, n + 1);
            assert_eq!(
                v.manifest.revision,
                if v.manifest.deleted { 621 + n } else { n + 1 }
            );
            if v.manifest.deleted {
                deleted += 1;
                assert!(v.payload.is_empty());
            } else {
                live += 1;
                assert_eq!(v.payload, b"catalogue-value");
            }
        }
    }
    assert_eq!(d.count(&scope()).unwrap(), live + deleted);
    println!("{{\"operation\":\"{}\",\"elapsed_ms\":{elapsed:.6},\"completed\":{completed},\"active\":{active},\"live\":{live},\"deleted\":{deleted},\"head_reads\":{},\"manifest_reads\":{},\"value_reads\":{},\"manifest_bytes\":{},\"payload_bytes\":{},\"authority_checks\":{}}}",args[2],s.head_reads(),s.manifest_reads(),s.resolve_reads(),s.manifest_bytes(),s.payload_bytes(),a.checks);
    Ok(())
}
