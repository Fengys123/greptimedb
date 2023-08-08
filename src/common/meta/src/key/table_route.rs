// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

use api::v1::meta::{Peer, TableName, TableRoute, TableRouteValue};
use lazy_static::__Deref;
use snafu::{ensure, ResultExt};
use table::metadata::TableId;

use super::TableMetaKey;
use crate::error::{self, Result};
use crate::key::to_removed_key;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, MoveValueRequest};

pub const TABLE_ROUTE_PREFIX: &str = "__meta_table_route";

#[derive(Debug, Clone)]
pub struct TableRouteInfo {
    peers: Vec<Peer>,
    table_route: TableRoute,
}

impl TableRouteInfo {
    pub fn new(peers: Vec<Peer>, table_route: TableRoute) -> TableRouteInfo {
        TableRouteInfo { peers, table_route }
    }
}

pub struct TableRouteManager {
    kv_backend: KvBackendRef,
}

impl TableRouteManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    pub async fn get(&self, table_id: TableId) -> Result<Option<TableRouteValue>> {
        let raw_key = TableRouteKey2::new(table_id).as_raw_key();

        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|kv| {
                kv.value
                    .deref()
                    .try_into()
                    .context(error::DecodeTableRouteSnafu)
            })
            .transpose()
    }

    pub async fn create(&self, table_id: TableId, table_route: TableRouteInfo) -> Result<()> {
        let result = self
            .compare_and_put(table_id, None, table_route.clone())
            .await?;

        if let Err(curr) = result {
            let Some(curr) = curr else {
                return error::UnexpectedSnafu {
                    err_msg: format!("compare_and_put expect None but failed with current value None, table_id: {table_id}, table_route_value: {table_route:?}"),
                }.fail()
            };
            ensure!(
                curr.table_route == Some(table_route.table_route)
                    && curr.peers == table_route.peers,
                error::UnexpectedSnafu {
                    err_msg: format!(
                        "TableRouteValue for table {table_id} is updated before it is created!"
                    )
                }
            );
        }
        Ok(())
    }

    pub async fn compare_and_put(
        &self,
        table_id: TableId,
        expected: Option<TableRouteValue>,
        new_table_route: TableRouteInfo,
    ) -> Result<std::result::Result<(), Option<TableRouteValue>>> {
        let key = TableRouteKey2::new(table_id);
        let raw_key = key.as_raw_key();

        let (version, raw_expected) = if let Some(expected) = expected {
            (expected.version, expected.into())
        } else {
            (0, vec![])
        };
        let table_route_value = TableRouteValue {
            peers: new_table_route.peers,
            table_route: Some(new_table_route.table_route),
            version: version + 1,
        };

        let new_raw_value: Vec<u8> = table_route_value.into();
        let req = CompareAndPutRequest::new()
            .with_key(raw_key)
            .with_expect(raw_expected)
            .with_value(new_raw_value);

        let resp = self.kv_backend.compare_and_put(req).await?;

        if resp.success {
            return Ok(Ok(()));
        }

        let prev = resp
            .prev_kv
            .map(|kv| {
                kv.value
                    .deref()
                    .try_into()
                    .context(error::DecodeTableRouteSnafu)
            })
            .transpose()?;

        Ok(Err(prev))
    }

    pub async fn remove(&self, table_id: TableId) -> Result<()> {
        let key = TableRouteKey2::new(table_id).as_raw_key();
        let removed_key = to_removed_key(&String::from_utf8_lossy(&key));
        let req = MoveValueRequest::new(key, removed_key.as_bytes());

        self.kv_backend.move_value(req).await?;

        Ok(())
    }
}

#[derive(Copy, Clone)]
pub struct TableRouteKey2 {
    pub table_id: TableId,
}

impl TableMetaKey for TableRouteKey2 {
    fn as_raw_key(&self) -> Vec<u8> {
        format!("{}/{}", TABLE_ROUTE_PREFIX, self.table_id).into_bytes()
    }
}

impl TableRouteKey2 {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

#[derive(Copy, Clone)]
pub struct TableRouteKey<'a> {
    pub table_id: TableId,
    pub catalog_name: &'a str,
    pub schema_name: &'a str,
    pub table_name: &'a str,
}

impl<'a> TableRouteKey<'a> {
    pub fn with_table_name(table_id: TableId, t: &'a TableName) -> Self {
        Self {
            table_id,
            catalog_name: &t.catalog_name,
            schema_name: &t.schema_name,
            table_name: &t.table_name,
        }
    }

    pub fn prefix(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            TABLE_ROUTE_PREFIX, self.catalog_name, self.schema_name, self.table_name
        )
    }

    pub fn removed_key(&self) -> String {
        to_removed_key(&self.to_string())
    }
}

impl<'a> Display for TableRouteKey<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.prefix(), self.table_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::{Table, TableName, TableRoute, TableRouteValue};

    use super::TableRouteKey;
    use crate::error;
    use crate::key::table_route::{TableRouteInfo, TableRouteManager};
    use crate::kv_backend::memory::MemoryKvBackend;

    #[test]
    fn test_table_route_key() {
        let key = TableRouteKey {
            table_id: 123,
            catalog_name: "greptime",
            schema_name: "public",
            table_name: "demo",
        };

        let prefix = key.prefix();
        assert_eq!("__meta_table_route-greptime-public-demo", prefix);

        let key_string = key.to_string();
        assert_eq!("__meta_table_route-greptime-public-demo-123", key_string);

        let removed = key.removed_key();
        assert_eq!(
            "__removed-__meta_table_route-greptime-public-demo-123",
            removed
        );
    }

    #[test]
    fn test_with_table_name() {
        let table_name = TableName {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "demo".to_string(),
        };

        let key = TableRouteKey::with_table_name(123, &table_name);

        assert_eq!(123, key.table_id);
        assert_eq!("greptime", key.catalog_name);
        assert_eq!("public", key.schema_name);
        assert_eq!("demo", key.table_name);
    }

    #[tokio::test]
    async fn test_table_route_manager() {
        let kv_backend: Arc<MemoryKvBackend<error::Error>> = Arc::new(MemoryKvBackend::default());

        let manager = TableRouteManager::new(kv_backend.clone());

        // test create
        let table_route_info_1 = mock_table_route_info(99, vec![1]);
        let table_route_info_2 = mock_table_route_info(99, vec![2]);

        assert!(manager.create(99, table_route_info_1.clone()).await.is_ok());

        let table_route_value = manager.get(99).await.unwrap().unwrap();
        assert_eq!(1, table_route_value.version);

        assert!(manager.create(99, table_route_info_1).await.is_ok());

        let table_route_value = manager.get(99).await.unwrap().unwrap();
        assert_eq!(1, table_route_value.version);

        assert!(manager
            .create(99, table_route_info_2.clone())
            .await
            .is_err());

        // test cap
        let table_route_val_1 = mock_table_route_value(99, vec![1], 1);
        let table_route_val_2 = mock_table_route_value(99, vec![2], 1);

        let cap_result = manager
            .compare_and_put(
                99,
                Some(table_route_val_2.clone()),
                table_route_info_2.clone(),
            )
            .await
            .unwrap();
        assert!(cap_result.is_err());
        assert_eq!(table_route_val_1, cap_result.err().unwrap().unwrap());

        assert!(manager
            .compare_and_put(99, Some(table_route_val_1), table_route_info_2,)
            .await
            .unwrap()
            .is_ok());

        let expected = mock_table_route_value(99, vec![2], 2);
        assert_eq!(expected, manager.get(99).await.unwrap().unwrap());

        // test remove
        assert!(manager.remove(99).await.is_ok());

        assert!(manager.get(99).await.unwrap().is_none());
    }

    fn mock_table_route_info(table_id: u64, table_schema: Vec<u8>) -> TableRouteInfo {
        let table_route = TableRoute {
            table: Some(Table {
                id: table_id,
                table_name: None,
                table_schema,
            }),
            region_routes: vec![],
        };

        TableRouteInfo {
            peers: vec![],
            table_route,
        }
    }

    fn mock_table_route_value(
        table_id: u64,
        table_schema: Vec<u8>,
        version: u64,
    ) -> TableRouteValue {
        let table_route = Some(TableRoute {
            table: Some(Table {
                id: table_id,
                table_name: None,
                table_schema,
            }),
            region_routes: vec![],
        });

        TableRouteValue {
            peers: vec![],
            table_route,
            version,
        }
    }
}
