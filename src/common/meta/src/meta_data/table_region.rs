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

use std::fmt::{Display, Formatter};
use std::mem::ManuallyDrop;

use regex::Regex;
use serde::Serializer;

use super::common::ALPHANUMERICS_NAME_PATTERN;

pub const TABLE_REGIONAL_KEY_PREFIX: &str = "__tr";

/// Table regional info that varies between datanode, so it contains a `node_id` field.
#[allow(dead_code)]
pub struct TableRegionalKey<'a> {
    pub catalog_name: &'a str,
    pub schema_name: &'a str,
    pub table_name: &'a str,
    pub node_id: u64,
}

impl Display for TableRegionalKey<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(TABLE_REGIONAL_KEY_PREFIX)?;
        f.write_str("-")?;
        f.write_str(self.catalog_name)?;
        f.write_str("-")?;
        f.write_str(self.schema_name)?;
        f.write_str("-")?;
        f.write_str(self.table_name)?;
        f.write_str("-")?;
        f.serialize_u64(self.node_id)
    }
}

lazy_static::lazy_static! {
    static ref TABLE_REGIONAL_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{TABLE_REGIONAL_KEY_PREFIX}-({ALPHANUMERICS_NAME_PATTERN})-({ALPHANUMERICS_NAME_PATTERN})-({ALPHANUMERICS_NAME_PATTERN})-([0-9]+)$"
    ))
    .unwrap();
}

impl<'a> TableRegionalKey<'a> {
    pub fn parse(key: &'a str) -> Result<TableRegionalKey<'a>, String> {
        // let captures = TABLE_REGIONAL_KEY_PATTERN.captures(key).unwrap();
        //
        // let cap = ManuallyDrop::new(captures);
        // let s = cap.get(1).unwrap();
        // // cap.name
        // let s1 = &cap[1];
        // let s2 = &cap[2];
        // let s3 = &cap[3];
        //
        // // ensure!(captures.len() == 5, InvalidCatalogSnafu { key });
        //
        // let node_id = cap[4].to_string().parse().unwrap();
        //
        // // std::mem::forget(captures);
        // Ok(Self {
        //     catalog_name: s1,
        //     schema_name: s2,
        //     table_name: s3,
        //     node_id,
        // })
        todo!()
    }
}

impl From<TableRegionalKey<'_>> for Vec<u8> {
    fn from(key: TableRegionalKey) -> Self {
        key.to_string().into()
    }
}

pub struct OwnedTableRegionKey {
    pub catalog_name: String,
    pub schema_name: String,
    pub table_name: String,
    pub node_id: u64,
}

impl TryFrom<Vec<u8>> for OwnedTableRegionKey {
    type Error = String;

    fn try_from(_value: Vec<u8>) -> Result<Self, Self::Error> {
        todo!()
    }
}
