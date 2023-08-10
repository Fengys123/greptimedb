use crate::kv_backend::txn::{Compare, CompareOp, TxnOp, TxnRequest};

impl TxnRequest {
    pub fn build_compare_and_put_txn(key: Vec<u8>, expect: Vec<u8>, value: Vec<u8>) -> TxnRequest {
        let compare = vec![Compare::with_value(key.clone(), CompareOp::Equal, expect)];
        let success = vec![TxnOp::Put(key.clone(), value)];
        let failure = vec![TxnOp::Get(key)];

        TxnRequest {
            compare,
            success,
            failure,
        }
    }
}
