use async_stream::try_stream;
use futures::Stream;
use tikv_client::{Key, TransactionClient};

use crate::{
    KvComponent, component_data_path, entity_handler::EntityHandler, error::Error, next_key,
    utils::key_to_string,
};

#[derive(Clone)]
pub struct DB {
    pub(crate) client: TransactionClient,
}

impl DB {
    pub async fn new(pd_endpoints: Vec<String>) -> Result<Self, Error> {
        let client = TransactionClient::new(pd_endpoints)
            .await
            .map_err(Error::TikvError)?;
        Ok(Self { client })
    }

    pub fn entity(&self, entity_id: impl Into<EntityID>) -> EntityHandler {
        EntityHandler {
            entity_id: entity_id.into(),
            client: self.client.clone(),
        }
    }

    pub async fn resource(&self) -> EntityHandler {
        EntityHandler {
            entity_id: EntityID::resource(),
            client: self.client.clone(),
        }
    }

    pub fn query<T: KvComponent + prost::Message + Default>(&self) -> T::Query {
        T::query(self.clone())
    }

    pub fn iterator<T: KvComponent + prost::Message + Default + 'static>(
        &self,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<(EntityID, T), Error>> + '_>> {
        const PAGE_SIZE: usize = 128;
        Box::pin(try_stream! {
            let mut snapshot = self.client.snapshot(
                self.client
                    .current_timestamp()
                    .await
                    .map_err(Error::TikvError)?,
                tikv_client::TransactionOptions::new_optimistic(),
            );

            let mut start_key: Key = component_data_path(T::type_path(), &EntityID::Empty).into();
            let end_key: Key = component_data_path(T::type_path(), &EntityID::Max).into();
            loop {
                let kvs = snapshot
                    .scan(start_key.clone()..end_key.clone(), PAGE_SIZE as u32)
                    .await
                    .map_err(Error::TikvError)?.collect::<Vec<_>>();
                if kvs.is_empty() {
                    break;
                }
                start_key = next_key(&kvs.last().ok_or(Error::NotFound)?.key().clone());
                let len = kvs.len();
                for kv in kvs {
                    let key = key_to_string(kv.key())?
                        .split('/')
                        .nth(3)
                        .ok_or(Error::NotFound)?
                        .to_string();
                    let Some(entity_id) = key.strip_prefix("e-") else {
                        continue;
                    };
                    let value = T::decode(kv.value().as_slice()).map_err(Error::DeserializationError)?;
                    yield (EntityID::new(entity_id.to_string()), value);
                }
                if len < PAGE_SIZE {
                    break;
                }
            }
        })
    }

    pub async fn keys(&self) -> Result<(), Error> {
        const PAGE_SIZE: usize = 128;
        let mut tnx = self
            .client
            .begin_with_options(
                tikv_client::TransactionOptions::new_optimistic()
                    .drop_check(tikv_client::CheckLevel::Warn),
            )
            .await
            .map_err(Error::TikvError)?;
        let mut start_key: Key = format!("").into();
        let end_key: Key = format!("~").into();

        loop {
            let kvs = tnx
                .scan_keys(start_key.clone()..end_key.clone(), PAGE_SIZE as u32)
                .await
                .map_err(Error::TikvError)?
                .collect::<Vec<_>>();
            if kvs.is_empty() {
                break;
            }
            start_key = next_key(&kvs.last().ok_or(Error::NotFound)?.clone());
            let len = kvs.len();
            for kv in kvs {
                let key = key_to_string(&kv)?;
                log::info!("{}", key);
            }
            if len < PAGE_SIZE {
                break;
            }
        }
        tnx.commit().await.map_err(Error::TikvError)?;
        Ok(())
    }
}

#[derive(Clone)]
pub enum EntityID {
    Resource,
    Entity(String),
    Empty,
    Max,
}

impl EntityID {
    pub fn new(entity_id: String) -> Self {
        if entity_id.find("/").is_some() {
            panic!("entity_id must not contain '/'");
        }
        Self::Entity(entity_id)
    }

    pub fn resource() -> Self {
        Self::Resource
    }

    pub(crate) fn new_raw(entity_id: String) -> Self {
        if let Some(entity_id) = entity_id.strip_prefix("e-") {
            Self::Entity(entity_id.to_string())
        } else {
            Self::Entity(entity_id)
        }
    }
}

const RESOURCE_ID: &str = "resource";

impl Into<String> for EntityID {
    fn into(self) -> String {
        match self {
            EntityID::Resource => RESOURCE_ID.to_string(),
            EntityID::Entity(entity_id) => format!("e-{}", entity_id),
            EntityID::Empty => "".to_string(),
            EntityID::Max => "~".to_string(),
        }
    }
}

impl Into<EntityID> for &str {
    fn into(self) -> EntityID {
        if self.find("/").is_some() {
            panic!("entity_id must not contain '/'");
        }
        EntityID::Entity(self.to_string())
    }
}

impl Into<EntityID> for String {
    fn into(self) -> EntityID {
        if self.find("/").is_some() {
            panic!("entity_id must not contain '/'");
        }
        EntityID::Entity(self)
    }
}

impl std::fmt::Debug for EntityID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Into::<String>::into(self.clone()))
    }
}
