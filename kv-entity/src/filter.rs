use std::marker::PhantomData;

use crate::{
    DB, KvComponent, component_data_path, component_index_path, db::EntityHandler, error::Error,
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

    async fn query_entity_id(&self, snapshot: &mut tikv_client::Snapshot) -> Result<String, Error> {
        let (start, end) = match &self.bound_condition {
            BoundCondition::Value(value) => (value, value),
            BoundCondition::Range(start, end) => (start, end),
        };

        let start_key = component_index_path(T::type_path(), &self.field_name, start, "");
        let end_key = component_index_path(T::type_path(), &self.field_name, end, "~");
        let Some(kv) = snapshot
            .scan(start_key..end_key.into(), 1)
            .await
            .map_err(Error::TikvError)?
            .next()
        else {
            return Err(Error::NotFound);
        };
        return Ok(String::from_utf8(kv.value().to_vec())
            .map_err(|e| Error::InvalidEntityId(e.to_string()))?);
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

    pub async fn all(&self) -> Result<Vec<T>, Error> {
        Ok(Vec::new())
    }
}
