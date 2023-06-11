use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion::catalog::catalog::CatalogList;
use datafusion_expr::LogicalPlan;

use crate::{error::Error, SubstraitPlan};

pub struct DFLogicalSubstraitConvertor;

#[async_trait]
impl SubstraitPlan for DFLogicalSubstraitConvertor {
    type Error = Error;

    type Plan = LogicalPlan;

    async fn decode<B: Buf + Send>(
        &self,
        _message: B,
        _catalog_list: Arc<dyn CatalogList>,
    ) -> Result<Self::Plan, Self::Error> {
        unimplemented!()
    }

    fn encode(&self, _plan: Self::Plan) -> Result<Bytes, Self::Error> {
        unimplemented!()
    }
}
