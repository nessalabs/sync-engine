//! Owned metadata fixtures, deliberately not a deployed publication adapter.
use nessa_sync::replication::{
    application::{RecordHeadSource, RecordSource, SourceError},
    catalogue::{
        CatalogueHeadSource, CataloguePass, CatalogueSource, CatalogueSourceError, ManifestPage,
        ManifestRequest, ResolvedEntry,
    },
    domain::{Id, Page, PageRequest, Scope, SourceHead, SOURCE_HEAD_VERSION},
    infrastructure::{SqliteCatalogueSource, SqliteReferenceSource},
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
pub fn id(s: &str) -> Id {
    Id::new(s).unwrap()
}
pub fn scope() -> Scope {
    Scope::new(
        id("reader"),
        id("origin"),
        id("stream"),
        id("first"),
        id("opaque-v1"),
        id("epoch"),
    )
}
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "owned-source-head-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    pub fn records(&self) -> Records {
        Records {
            inner: SqliteReferenceSource::open(
                self.path("records.db"),
                id("origin"),
                id("stream"),
                id("first"),
                id("opaque-v1"),
            )
            .unwrap(),
            meta: Metadata::default(),
        }
    }
    pub fn catalogue(&self) -> Catalogue {
        Catalogue {
            inner: SqliteCatalogueSource::open(
                self.path("catalogue.db"),
                id("origin"),
                id("stream"),
                id("first"),
                id("opaque-v1"),
            )
            .unwrap(),
            meta: Metadata::default(),
        }
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
pub struct Metadata {
    pub version: u16,
    pub reader: u64,
    pub writer: u64,
    pub echo: Option<Scope>,
    pub position: Option<u64>,
}
impl Default for Metadata {
    fn default() -> Self {
        Self {
            version: SOURCE_HEAD_VERSION,
            reader: 0,
            writer: 0,
            echo: None,
            position: None,
        }
    }
}
impl Metadata {
    fn head(&self, scope: &Scope, position: u64) -> SourceHead {
        SourceHead::advertised(
            self.echo.clone().unwrap_or_else(|| scope.clone()),
            self.version,
            self.position.unwrap_or(position),
            self.reader,
            self.writer,
        )
    }
}
pub struct Records {
    pub inner: SqliteReferenceSource,
    pub meta: Metadata,
}
impl RecordSource for Records {
    fn head(&mut self, scope: &Scope) -> Result<u64, SourceError> {
        self.inner.head(scope)
    }
    fn page(&mut self, request: &PageRequest) -> Result<Page, SourceError> {
        self.inner.page(request)
    }
}
impl RecordHeadSource for Records {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, SourceError> {
        self.inner.head(scope).map(|n| self.meta.head(scope, n))
    }
}
pub struct Catalogue {
    pub inner: SqliteCatalogueSource,
    pub meta: Metadata,
}
impl CatalogueSource for Catalogue {
    fn head(&mut self, scope: &Scope) -> Result<u64, CatalogueSourceError> {
        self.inner.head(scope)
    }
    fn manifest(
        &mut self,
        request: &ManifestRequest,
    ) -> Result<ManifestPage, CatalogueSourceError> {
        self.inner.manifest(request)
    }
    fn resolve(
        &mut self,
        pass: &CataloguePass,
        entry: &Id,
        maximum: usize,
    ) -> Result<ResolvedEntry, CatalogueSourceError> {
        self.inner.resolve(pass, entry, maximum)
    }
}
impl CatalogueHeadSource for Catalogue {
    fn source_head(&mut self, scope: &Scope) -> Result<SourceHead, CatalogueSourceError> {
        self.inner.head(scope).map(|n| self.meta.head(scope, n))
    }
}
