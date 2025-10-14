mod bundle;
mod db;
mod entity_handler;
mod error;
mod filter;
mod meta;
mod utils;

pub use db::DB;
pub use entity_handler::EntityHandler;
pub use error::Error;
pub use filter::{BoundCondition, Filter};
pub use kv_entity_derive::{KvComponent, KvRelation};
pub use utils::{ComponentMeta, RelationDirection};
pub(crate) use utils::{
    component_data_path, component_index_path, entity_metadata_path, next_key, relation_data_path,
    relation_edge_no_type_path, relation_edge_path,
};

/// KvComponent trait 定义了 KV 存储实体的基本接口
pub trait KvComponent {
    type Query;

    /// 返回类型的完整路径，用作 KV 存储的前缀
    fn type_path() -> TypePath;

    /// 返回查询器
    fn query(client: DB) -> Self::Query;

    /// 返回索引字段和对应的值
    fn indexed_fields(&self) -> Vec<(String, String)>;

    /// 返回索引字段名
    fn indexed_field_names() -> Vec<&'static str>;
}

pub trait KvRelation {
    /// 返回类型的完整路径，用作 KV 存储的前缀
    fn type_path() -> TypePath;
}

#[derive(Clone, Copy, Debug)]
pub struct TypePath(pub &'static str);
