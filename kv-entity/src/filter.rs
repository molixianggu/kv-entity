use std::marker::PhantomData;

use tikv_client::Key;

use crate::{
    DB, KvComponent, component_data_path, component_index_path,
    db::EntityID,
    entity_handler::{EntityHandler, EntityListHandler},
    error::Error,
    next_key,
};

pub enum BoundCondition {
    Value(String),
    Range(String, String),
}

pub struct Filter<T> {
    client: DB,
    field_name: String,
    bound_condition: BoundCondition,
    _marker: PhantomData<T>,
}

impl<T> Filter<T>
where
    T: KvComponent + prost::Message + Default,
{
    pub fn new(client: DB, field_name: String, bound_condition: BoundCondition) -> Self {
        Self {
            client,
            field_name,
            bound_condition,
            _marker: PhantomData,
        }
    }

    async fn query_entity_id(
        &self,
        snapshot: &mut tikv_client::Snapshot,
    ) -> Result<EntityID, Error> {
        let (start, end) = match &self.bound_condition {
            BoundCondition::Value(value) => (value, value),
            BoundCondition::Range(start, end) => (start, end),
        };

        let start_key =
            component_index_path(T::type_path(), &self.field_name, start, &EntityID::Empty);
        let end_key = component_index_path(T::type_path(), &self.field_name, end, &EntityID::Max);

        let Some(kv) = snapshot
            .scan(start_key..end_key.into(), 1)
            .await
            .map_err(Error::TikvError)?
            .next()
        else {
            return Err(Error::NotFound);
        };
        return Ok(EntityID::new_raw(
            String::from_utf8(kv.value().to_vec())
                .map_err(|e| Error::InvalidEntityId(e.to_string()))?,
        ));
    }

    async fn query_entity_id_vec(
        &self,
        snapshot: &mut tikv_client::Snapshot,
    ) -> Result<Vec<EntityID>, Error> {
        let (start, end) = match &self.bound_condition {
            BoundCondition::Value(value) => (value, value),
            BoundCondition::Range(start, end) => (start, end),
        };

        const PAGE_SIZE: usize = 128;

        let mut start_key: Key =
            component_index_path(T::type_path(), &self.field_name, start, &EntityID::Empty).into();
        let end_key: Key =
            component_index_path(T::type_path(), &self.field_name, end, &EntityID::Max).into();
        let mut entity_ids = Vec::new();

        loop {
            let kvs = snapshot
                .scan(start_key.clone()..end_key.clone(), PAGE_SIZE as u32)
                .await
                .map_err(Error::TikvError)?
                .collect::<Vec<_>>();

            if kvs.is_empty() {
                break;
            }

            start_key = next_key(&kvs.last().ok_or(Error::NotFound)?.key().clone());
            let len = kvs.len();

            for kv in kvs {
                entity_ids.push(EntityID::new_raw(
                    String::from_utf8(kv.value().to_vec())
                        .map_err(|e| Error::InvalidEntityId(e.to_string()))?,
                ));
            }

            if len < PAGE_SIZE {
                break;
            }
        }
        return Ok(entity_ids);
    }

    pub async fn entity(&self) -> Result<EntityHandler, Error> {
        let mut snapshot = self.client.client.snapshot(
            self.client
                .client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );
        let entity_id = self.query_entity_id(&mut snapshot).await?;
        Ok(EntityHandler {
            entity_id,
            client: self.client.client.clone(),
        })
    }

    pub async fn single(&self) -> Result<T, Error> {
        let mut snapshot = self.client.client.snapshot(
            self.client
                .client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );
        let entity_id = self.query_entity_id(&mut snapshot).await?;
        let Some(data) = snapshot
            .get(component_data_path(T::type_path(), &entity_id))
            .await
            .map_err(Error::TikvError)?
        else {
            return Err(Error::NotFound);
        };

        let value = T::decode(data.as_slice()).map_err(Error::DeserializationError)?;
        return Ok(value);
    }

    pub async fn count(&self) -> Result<u64, Error> {
        let mut snapshot = self.client.client.snapshot(
            self.client
                .client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );
        Ok(self.query_entity_id_vec(&mut snapshot).await?.len() as u64)
    }

    pub async fn all(&self) -> Result<Vec<T>, Error> {
        let mut snapshot = self.client.client.snapshot(
            self.client
                .client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );
        let entity_ids = self.query_entity_id_vec(&mut snapshot).await?;

        snapshot
            .batch_get(
                entity_ids
                    .iter()
                    .map(|id| component_data_path(T::type_path(), id))
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(Error::TikvError)?
            .map(|data| T::decode(data.value().as_slice()).map_err(Error::DeserializationError))
            .collect()
    }

    pub async fn list(&self) -> Result<EntityListHandler, Error> {
        let mut snapshot = self.client.client.snapshot(
            self.client
                .client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );
        let entity_ids = self.query_entity_id_vec(&mut snapshot).await?;
        Ok(EntityListHandler {
            entity_ids,
            client: self.client.client.clone(),
        })
    }
}
