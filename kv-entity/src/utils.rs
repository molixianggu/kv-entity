use std::sync::RwLock;
use std::{collections::HashSet, sync::LazyLock};
use tikv_client::Key;

use crate::{Error, TypePath, db::EntityID};

pub(crate) fn next_key(key: &Key) -> Key {
    let mut next_key = Into::<Vec<u8>>::into(key.clone());
    for i in (0..next_key.len()).rev() {
        if next_key[i] < 0xff {
            next_key[i] += 1;
            return Key::from(next_key);
        }
    }
    Key::from(next_key)
}

pub(crate) fn component_data_path(type_path: TypePath, entity_id: &EntityID) -> String {
    format!("component/single/{}/{:?}", type_path.0, entity_id)
}

pub(crate) fn entity_metadata_path(entity_id: &EntityID) -> String {
    format!("entity/metadata/{:?}", entity_id)
}

pub(crate) fn component_index_path(
    type_path: TypePath,
    field_name: &str,
    value: &str,
    entity_id: &EntityID,
) -> String {
    format!(
        "component/index/{}/{}/{}/{:?}",
        type_path.0, field_name, value, entity_id
    )
}

#[derive(Clone, Copy, Debug)]
pub enum RelationDirection {
    Both,
    In,
    Out,
}

impl std::ops::Not for RelationDirection {
    type Output = RelationDirection;
    fn not(self) -> Self::Output {
        match self {
            RelationDirection::In => RelationDirection::Out,
            RelationDirection::Out => RelationDirection::In,
            RelationDirection::Both => RelationDirection::Both,
        }
    }
}

pub(crate) fn relation_edge_path(
    type_path: TypePath,
    a: &EntityID,
    b: &EntityID,
    direction: RelationDirection,
) -> String {
    let io = match direction {
        RelationDirection::In => "in",
        RelationDirection::Out => "out",
        RelationDirection::Both => {
            return format!("relation/edge/{:?}/{}/{:?}", a, type_path.0, b);
        }
    };
    format!("relation/edge/{:?}/{}/{}/{:?}", a, type_path.0, io, b)
}

pub(crate) fn relation_data_path(type_path: TypePath, a: &EntityID, b: &EntityID) -> String {
    format!("relation/data/{}/{:?}/{:?}", type_path.0, a, b)
}

pub(crate) fn relation_edge_no_type_path(entity_id: &EntityID, type_path: TypePath) -> String {
    format!("relation/edge/{:?}/{}", entity_id, type_path.0)
}

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
#[allow(unused)]
pub fn all_components() -> std::collections::HashMap<&'static str, Vec<&'static str>> {
    inventory::iter::<ComponentMeta>()
        .map(|meta| (meta.type_path, (meta.indexed_field_names)()))
        .collect()
}

pub(crate) fn key_to_string(key: &Key) -> Result<String, Error> {
    Ok(String::from_utf8(Into::<Vec<u8>>::into(key.clone())).map_err(Error::InvalidUtf8)?)
}

// 全局字符串缓存池
static STRING_CACHE: LazyLock<RwLock<HashSet<&'static str>>> =
    LazyLock::new(|| RwLock::new(HashSet::new()));

pub(crate) fn intern_string(s: &str) -> &'static str {
    {
        let cache = STRING_CACHE.read().unwrap();
        for &cached in cache.iter() {
            if cached == s {
                return cached;
            }
        }
    }
    let mut cache = STRING_CACHE.write().unwrap();

    for &cached in cache.iter() {
        if cached == s {
            return cached;
        }
    }
    let leaked: &'static str = Box::leak(s.to_string().into_boxed_str());
    cache.insert(leaked);
    leaked
}
