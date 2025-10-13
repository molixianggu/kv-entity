use prost::Message;
use tikv_client::{TransactionClient, proto::kvrpcpb};

use crate::{
    KvComponent,
    bundle::ComponentBundle,
    component_data_path, component_index_path, entity_metadata_path,
    error::Error,
    meta::{ComponentArchetype, EntityMetadata},
};

#[derive(Clone)]
pub struct DB {
    pub(crate) client: TransactionClient,
}

#[derive(Clone)]
pub struct EntityHandler {
    pub(crate) entity_id: String,
    pub(crate) client: TransactionClient,
}

impl DB {
    pub async fn new(pd_endpoints: Vec<String>) -> Result<Self, Error> {
        let client = TransactionClient::new(pd_endpoints)
            .await
            .map_err(Error::TikvError)?;
        Ok(Self { client })
    }

    pub fn entity(&self, entity_id: impl Into<String>) -> EntityHandler {
        EntityHandler {
            entity_id: format!("e-{}", entity_id.into()),
            client: self.client.clone(),
        }
    }

    const RESOURCE_ID: &str = "resource";

    pub async fn resource(&self) -> EntityHandler {
        EntityHandler {
            entity_id: Self::RESOURCE_ID.to_string(),
            client: self.client.clone(),
        }
    }

    pub fn query<T: KvComponent + prost::Message + Default>(&self) -> T::Query {
        T::query(self.clone())
    }

    pub async fn iterator<T: KvComponent + prost::Message + Default>(
        &self,
    ) -> Result<Vec<(String, T)>, Error> {
        let mut snapshot = self.client.snapshot(
            self.client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );

        let mut data = Vec::new();

        let start_key = component_data_path(T::type_path(), "");
        let end_key = component_data_path(T::type_path(), "~");
        for kv in snapshot
            .scan(start_key.clone()..end_key.clone().into(), 100)
            .await
            .map_err(Error::TikvError)?
        {
            let key = String::from_utf8(Into::<Vec<u8>>::into(kv.key().clone()))
                .map_err(Error::InvalidUtf8)?
                .split('/')
                .nth(3)
                .ok_or(Error::NotFound)?
                .to_string();
            let Some(entity_id) = key.strip_prefix("e-") else {
                continue;
            };
            data.push((
                entity_id.to_string(),
                T::decode(kv.value().as_slice()).map_err(Error::DeserializationError)?,
            ));
        }

        Ok(data)
    }
}

impl EntityHandler {
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    pub async fn get<T: KvComponent + prost::Message + Default>(&self) -> Result<Option<T>, Error> {
        let mut snapshot = self.client.snapshot(
            self.client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );

        let Some(data) = snapshot
            .get(component_data_path(T::type_path(), &self.entity_id))
            .await
            .map_err(Error::TikvError)?
        else {
            return Ok(None);
        };

        let message = T::decode(data.as_slice()).map_err(Error::DeserializationError)?;

        Ok(Some(message))
    }

    pub async fn attach(&self, bundle: impl ComponentBundle) -> Result<Self, Error> {
        bundle.attach_to(self).await
    }

    pub async fn detach<T: KvComponent + prost::Message + Default>(&self) -> Result<Self, Error> {
        let mut txn: tikv_client::Transaction = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        if !T::indexed_field_names().is_empty() {
            let Some(mut metadata) = self.get_metadata(&mut txn).await? else {
                txn.rollback().await.map_err(Error::TikvError)?;
                return Err(Error::NotFound);
            };
            let mut mutations = Vec::new();
            for key in metadata
                .component_archetypes
                .remove(T::type_path())
                .ok_or(Error::NotFound)?
                .index_keys
                .values()
            {
                mutations.push(kvrpcpb::Mutation {
                    key: key.clone().into(),
                    op: kvrpcpb::Op::Del.into(),
                    ..Default::default()
                });
            }
            txn.batch_mutate(mutations)
                .await
                .map_err(Error::TikvError)?;
            self.update_metadata(&mut txn, metadata).await?;
        }

        txn.delete(component_data_path(T::type_path(), &self.entity_id))
            .await
            .map_err(Error::TikvError)?;

        txn.commit().await.map_err(Error::TikvError)?;

        Ok(self.clone())
    }

    pub async fn delete(&self) -> Result<Self, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        let Some(metadata) = self.get_metadata(&mut txn).await? else {
            txn.rollback().await.map_err(Error::TikvError)?;
            return Err(Error::NotFound);
        };
        let mut mutations = Vec::new();
        for (component_type, component_archetype) in metadata.component_archetypes.iter() {
            for (field, key) in component_archetype.index_keys.iter() {
                mutations.push(kvrpcpb::Mutation {
                    key: component_index_path(&component_type, field, key, &self.entity_id).into(),
                    op: kvrpcpb::Op::Del.into(),
                    ..Default::default()
                });
            }
            mutations.push(kvrpcpb::Mutation {
                key: component_data_path(&component_type, &self.entity_id).into(),
                op: kvrpcpb::Op::Del.into(),
                ..Default::default()
            });
        }
        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;
        self.delete_metadata(&mut txn).await?;
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
    }

    pub async fn metadata(&self) -> Result<EntityMetadata, Error> {
        let mut snapshot = self.client.snapshot(
            self.client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );
        let Some(metadata) = snapshot
            .get(entity_metadata_path(&self.entity_id))
            .await
            .map_err(Error::TikvError)?
        else {
            return Err(Error::NotFound);
        };
        Ok(EntityMetadata::decode(metadata.as_slice()).map_err(Error::DeserializationError)?)
    }

    pub(super) async fn get_metadata(
        &self,
        txn: &mut tikv_client::Transaction,
    ) -> Result<Option<EntityMetadata>, Error> {
        let Some(metadata) = txn
            .get(entity_metadata_path(&self.entity_id))
            .await
            .map_err(Error::TikvError)?
        else {
            return Ok(None);
        };

        Ok(Some(
            EntityMetadata::decode(metadata.as_slice()).map_err(Error::DeserializationError)?,
        ))
    }

    pub(super) async fn update_metadata(
        &self,
        txn: &mut tikv_client::Transaction,
        metadata: EntityMetadata,
    ) -> Result<(), Error> {
        txn.put(
            entity_metadata_path(&self.entity_id),
            metadata.encode_to_vec(),
        )
        .await
        .map_err(Error::TikvError)?;
        Ok(())
    }

    pub(super) async fn delete_metadata(
        &self,
        txn: &mut tikv_client::Transaction,
    ) -> Result<(), Error> {
        txn.delete(entity_metadata_path(&self.entity_id))
            .await
            .map_err(Error::TikvError)?;
        Ok(())
    }

    pub(crate) async fn attach_component_in_txn<T: KvComponent + prost::Message + Default>(
        &self,
        mutations: &mut Vec<kvrpcpb::Mutation>,
        metadata: &mut EntityMetadata,
        value: T,
    ) -> Result<(), Error> {
        {
            let indexed_fields = value
                .indexed_fields()
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>();

            let default_archetype = ComponentArchetype {
                index_keys: T::indexed_field_names()
                    .into_iter()
                    .map(|k| (k.to_string(), "".to_string()))
                    .collect(),
            };

            for (field, key) in metadata
                .component_archetypes
                .entry(T::type_path().to_string())
                .or_insert(default_archetype)
                .index_keys
                .iter_mut()
            {
                if !key.is_empty() {
                    mutations.push(kvrpcpb::Mutation {
                        key: component_index_path(T::type_path(), field, key, &self.entity_id)
                            .into(),
                        op: kvrpcpb::Op::Del.into(),
                        ..Default::default()
                    });
                }
                let Some(value) = indexed_fields.get(field.as_str()) else {
                    continue;
                };
                let new_key = component_index_path(T::type_path(), field, value, &self.entity_id);
                mutations.push(kvrpcpb::Mutation {
                    key: new_key.clone().into(),
                    op: kvrpcpb::Op::Put.into(),
                    value: self.entity_id.clone().into(),
                    ..Default::default()
                });
                *key = value.clone();
            }
        }

        let data = value.encode_to_vec();

        mutations.push(kvrpcpb::Mutation {
            key: component_data_path(T::type_path(), &self.entity_id).into(),
            op: kvrpcpb::Op::Put.into(),
            value: data.into(),
            ..Default::default()
        });

        Ok(())
    }
}
