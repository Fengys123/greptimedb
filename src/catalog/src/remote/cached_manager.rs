use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use moka::future::{Cache, CacheBuilder};
use table::TableRef;

use crate::error::Error::NoSuchKey;
use crate::error::{Error, GenericSnafu, NoSuchKeySnafu, Result};
use crate::{
    CatalogManager, CatalogManagerRef, CatalogProviderRef, DeregisterTableRequest,
    RegisterSchemaRequest, RegisterSystemTableRequest, RegisterTableRequest, RenameTableRequest,
    SchemaProviderRef,
};

/// [CachedCatalogManager] is a proxy struct of [CatalogManager] which supports caching the results
/// of some methods in [CatalogManager].
#[derive(Clone)]
pub struct CachedCatalogManager {
    catalog_cache: Arc<Cache<String, CatalogProviderRef>>,
    schema_cache: Arc<Cache<String, SchemaProviderRef>>,
    table_cache: Arc<Cache<String, TableRef>>,
    catalog_manager: CatalogManagerRef,
}

#[async_trait::async_trait]
impl CatalogManager for CachedCatalogManager {
    async fn start(&self) -> Result<()> {
        self.catalog_manager.start().await
    }

    async fn register_table(&self, request: RegisterTableRequest) -> Result<bool> {
        self.catalog_manager.register_table(request).await
    }

    async fn deregister_table(&self, request: DeregisterTableRequest) -> Result<bool> {
        let catalog = request.catalog.clone();
        let schema = request.schema.clone();
        let table_name = request.table_name.clone();

        let ret = self.catalog_manager.deregister_table(request).await;

        if ret.is_ok() {
            self.table_cache
                .invalidate(&format!("{catalog}-{schema}-{table_name}"))
                .await;
        }

        ret
    }

    async fn register_schema(&self, request: RegisterSchemaRequest) -> Result<bool> {
        self.catalog_manager.register_schema(request).await
    }

    async fn rename_table(&self, request: RenameTableRequest) -> Result<bool> {
        let catalog = request.catalog.clone();
        let schema = request.schema.clone();
        let table_name = request.table_name.clone();

        let ret = self.catalog_manager.rename_table(request).await;

        if ret.is_ok() {
            self.table_cache
                .invalidate(&format!("{catalog}-{schema}-{table_name}"))
                .await;
        }

        ret
    }

    async fn register_system_table(&self, request: RegisterSystemTableRequest) -> Result<()> {
        self.catalog_manager.register_system_table(request).await
    }

    async fn catalog(&self, catalog: &str) -> Result<Option<CatalogProviderRef>> {
        let init = async {
            let catalog_provider = self.catalog_manager.catalog(catalog).await;
            convert(catalog_provider)
        };

        let catalog_provider = self.catalog_cache.try_get_with_by_ref(catalog, init).await;

        re_convert(catalog_provider)
    }

    async fn schema(&self, catalog: &str, schema: &str) -> Result<Option<SchemaProviderRef>> {
        let init = async {
            let schema_provider = self.catalog_manager.schema(catalog, schema).await;
            convert(schema_provider)
        };

        let key = format!("{}-{}", catalog, schema);
        let schema_provider = self.schema_cache.try_get_with_by_ref(&key, init).await;

        re_convert(schema_provider)
    }

    async fn table(&self, catalog: &str, schema: &str, table: &str) -> Result<Option<TableRef>> {
        let init = async {
            let table = self.catalog_manager.table(catalog, schema, table).await;
            convert(table)
        };

        let key = format!("{}-{}-{}", catalog, schema, table);
        let table = self.table_cache.try_get_with_by_ref(&key, init).await;

        re_convert(table)
    }

    async fn catalog_names(&self) -> Result<Vec<String>> {
        self.catalog_manager.catalog_names().await
    }

    async fn register_catalog(
        &self,
        name: String,
        catalog: CatalogProviderRef,
    ) -> Result<Option<CatalogProviderRef>> {
        self.catalog_manager.register_catalog(name, catalog).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CachedCatalogManager {
    pub fn inner_catalog_manager(&self) -> CatalogManagerRef {
        self.catalog_manager.clone()
    }
}

// TODO(fys): Moka does not seem to have an "try_optionally_get_with" api.
// Related issue: https://github.com/moka-rs/moka/issues/254

pub fn convert<V>(v: std::result::Result<Option<V>, Error>) -> Result<V> {
    match v {
        Ok(Some(v)) => Ok(v),
        Ok(None) => NoSuchKeySnafu {}.fail(),
        Err(e) => GenericSnafu { msg: e.to_string() }.fail(),
    }
}

pub fn re_convert<V>(v: std::result::Result<V, Arc<Error>>) -> Result<Option<V>> {
    match v {
        Ok(v) => Ok(Some(v)),
        Err(e) => {
            if matches!(e.as_ref(), NoSuchKey { .. }) {
                Ok(None)
            } else {
                GenericSnafu { msg: e.to_string() }.fail()
            }
        }
    }
}

pub struct CachedCatalogManagerBuilder {
    catalog_cache_config: Option<(u64, Duration, Duration)>,
    schema_cache_config: Option<(u64, Duration, Duration)>,
    table_cache_config: Option<(u64, Duration, Duration)>,
    catalog_manager: CatalogManagerRef,
}

impl CachedCatalogManagerBuilder {
    pub fn new(catalog_manager: CatalogManagerRef) -> Self {
        Self {
            catalog_cache_config: None,
            schema_cache_config: None,
            table_cache_config: None,
            catalog_manager,
        }
    }

    pub fn set_catalog_cache(
        mut self,
        max_cache_capacity: u64,
        ttl: Duration,
        tti: Duration,
    ) -> Self {
        self.catalog_cache_config = Some((max_cache_capacity, ttl, tti));
        self
    }

    pub fn set_schema_cache(
        mut self,
        max_cache_capacity: u64,
        ttl: Duration,
        tti: Duration,
    ) -> Self {
        self.schema_cache_config = Some((max_cache_capacity, ttl, tti));
        self
    }

    pub fn set_table_cache(
        mut self,
        max_cache_capacity: u64,
        ttl: Duration,
        tti: Duration,
    ) -> Self {
        self.table_cache_config = Some((max_cache_capacity, ttl, tti));
        self
    }

    pub fn build(self) -> CachedCatalogManager {
        let default_config = (
            1024,
            Duration::from_secs(10 * 60),
            Duration::from_secs(5 * 60),
        );

        let catalog_cache_config = self.catalog_cache_config.unwrap_or(default_config);

        let catalog_cache = Arc::new(
            CacheBuilder::new(catalog_cache_config.0)
                .time_to_live(catalog_cache_config.1)
                .time_to_idle(catalog_cache_config.2)
                .build(),
        );

        let schema_cache_config = self.schema_cache_config.unwrap_or(default_config);

        let schema_cache = Arc::new(
            CacheBuilder::new(schema_cache_config.0)
                .time_to_live(schema_cache_config.1)
                .time_to_idle(schema_cache_config.2)
                .build(),
        );

        let table_cache_config = self.table_cache_config.unwrap_or(default_config);

        let table_cache = Arc::new(
            CacheBuilder::new(table_cache_config.0)
                .time_to_live(table_cache_config.1)
                .time_to_idle(table_cache_config.2)
                .build(),
        );

        let catalog_manager = self.catalog_manager.clone();

        CachedCatalogManager {
            catalog_cache,
            schema_cache,
            table_cache,
            catalog_manager,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{convert, re_convert};
    use crate::error::Error::{self, Generic, NoSuchKey};
    use crate::error::{GenericSnafu, NoSuchKeySnafu, Result};

    #[test]
    fn test_result_convert() {
        let v = Ok(Some(1));
        let r = convert(v);
        assert_eq!(1, r.unwrap());

        let v = Ok(None::<i32>);
        let r = convert(v);
        assert!(matches!(r, Err(NoSuchKey { .. })));

        let v: Result<Option<i32>> = GenericSnafu {
            msg: "This is a error",
        }
        .fail();
        let r = convert(v);
        assert!(matches!(r, Err(Generic { .. })));
    }

    #[test]
    fn test_result_reconvert() {
        let v = Ok(21);
        let r = re_convert(v);
        assert_eq!(21, r.unwrap().unwrap());

        let v: std::result::Result<Option<i32>, Arc<Error>> = Err(Arc::new(
            GenericSnafu {
                msg: "This is a error",
            }
            .build(),
        ));
        let r = re_convert(v);
        assert!(matches!(r, Err(Generic { .. })));

        let v: std::result::Result<Option<i32>, Arc<Error>> =
            Err(Arc::new(NoSuchKeySnafu {}.build()));
        let r = re_convert(v);
        assert!(r.unwrap().is_none());
    }
}
