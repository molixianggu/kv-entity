mod bundle;
mod db;
mod error;
mod filter;
mod meta;

pub use db::DB;
pub use error::Error;
pub use filter::{BoundCondition, Filter};
pub use kv_entity_derive::KvComponent;

// 定义组件元信息
pub struct ComponentMeta {
    pub type_path: &'static str,
    pub indexed_field_names: fn() -> Vec<&'static str>,
}

impl std::fmt::Debug for ComponentMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ComponentMeta {{ type_path: {}, indexed: {:?} }}",
            self.type_path,
            (self.indexed_field_names)()
        )
    }
}

// 使用 inventory 收集所有组件
inventory::collect!(ComponentMeta);

// 获取所有已注册的组件
pub fn all_components() -> std::collections::HashMap<&'static str, Vec<&'static str>> {
    inventory::iter::<ComponentMeta>()
        .map(|meta| (meta.type_path, (meta.indexed_field_names)()))
        .collect()
}

/// KvComponent trait 定义了 KV 存储实体的基本接口
pub trait KvComponent {
    type Query;

    /// 返回类型的完整路径，用作 KV 存储的前缀
    fn type_path() -> &'static str;

    /// 返回查询器
    fn query(client: DB) -> Self::Query;

    /// 返回索引字段和对应的值
    fn indexed_fields(&self) -> Vec<(String, String)>;

    /// 返回索引字段名
    fn indexed_field_names() -> Vec<&'static str>;
}

pub(crate) fn component_data_path(type_path: &str, entity_id: &str) -> String {
    format!("component/single/{}/{}", type_path, entity_id)
}

pub(crate) fn entity_metadata_path(entity_id: &str) -> String {
    format!("entity/metadata/{}", entity_id)
}

pub(crate) fn component_index_path(
    type_path: &str,
    field_name: &str,
    value: &str,
    entity_id: &str,
) -> String {
    format!(
        "component/index/{}/{}/{}/{}",
        type_path, field_name, value, entity_id
    )
}
