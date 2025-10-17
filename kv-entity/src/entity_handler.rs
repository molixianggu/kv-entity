use async_stream::try_stream;
use futures::Stream;
use prost::Message;
use tikv_client::{Key, TransactionClient, proto::kvrpcpb};

use crate::{
    Error, KvComponent, KvRelation, RelationDirection, TypePath,
    bundle::ComponentBundle,
    component_data_path, component_index_path,
    db::EntityID,
    entity_metadata_path,
    meta::{ComponentArchetype, EntityMetadata},
    next_key, relation_data_path, relation_edge_no_type_path, relation_edge_path,
    utils::{intern_string, key_to_string},
};

#[derive(Clone)]
pub struct EntityHandler {
    pub(crate) entity_id: EntityID,
    pub(crate) client: TransactionClient,
}

impl EntityHandler {
    pub fn entity_id(&self) -> &EntityID {
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
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;
        let mut mutations = Vec::new();
        bundle.attach_to(self, &mut txn, &mut mutations).await?;
        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
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
                .remove(T::type_path().0)
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

        let mut mutations = Vec::new();

        self.delete_in_txn(&mut txn, &mut mutations).await?;

        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;
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

    pub async fn link<T: KvRelation + prost::Message + Default>(
        &self,
        entity_id: impl Into<EntityID>,
        value: T,
    ) -> Result<Self, Error> {
        let entity_id = entity_id.into();
        let mutations = vec![
            kvrpcpb::Mutation {
                key: relation_edge_path(
                    T::type_path(),
                    &self.entity_id,
                    &entity_id,
                    RelationDirection::In,
                )
                .into(),
                op: kvrpcpb::Op::Put.into(),
                value: [].into(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                key: relation_edge_path(
                    T::type_path(),
                    &entity_id,
                    &self.entity_id,
                    RelationDirection::Out,
                )
                .into(),
                op: kvrpcpb::Op::Put.into(),
                value: [].into(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                key: relation_data_path(T::type_path(), &self.entity_id, &entity_id).into(),
                op: kvrpcpb::Op::Put.into(),
                value: value.encode_to_vec(),
                ..Default::default()
            },
        ];

        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;

        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
    }

    pub async fn unlink<T: KvRelation + prost::Message + Default>(
        &self,
        entity_id: impl Into<EntityID>,
    ) -> Result<Self, Error> {
        let entity_id = entity_id.into();
        let mutations = vec![
            kvrpcpb::Mutation {
                key: relation_edge_path(
                    T::type_path(),
                    &self.entity_id,
                    &entity_id,
                    RelationDirection::In,
                )
                .into(),
                op: kvrpcpb::Op::Del.into(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                key: relation_edge_path(
                    T::type_path(),
                    &entity_id,
                    &self.entity_id,
                    RelationDirection::Out,
                )
                .into(),
                op: kvrpcpb::Op::Del.into(),
                ..Default::default()
            },
            kvrpcpb::Mutation {
                key: relation_data_path(T::type_path(), &self.entity_id, &entity_id).into(),
                op: kvrpcpb::Op::Del.into(),
                ..Default::default()
            },
        ];

        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;

        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
    }

    pub async fn edges<T: KvRelation + prost::Message + Default + 'static>(
        &self,
        direction: RelationDirection,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<(EntityID, RelationDirection, T), Error>> + Send>>
    {
        const PAGE_SIZE: usize = 128;

        let client = self.client.clone();
        let self_entity_id = self.entity_id.clone();

        Box::pin(try_stream! {
            let mut snapshot = client.snapshot(
                client
                    .current_timestamp()
                    .await
                    .map_err(Error::TikvError)?,
                tikv_client::TransactionOptions::new_optimistic(),
            );
            let mut start_key: Key =
                relation_edge_path(T::type_path(), &self_entity_id, &EntityID::Empty, direction).into();
            let end_key: Key =
                relation_edge_path(T::type_path(), &self_entity_id, &EntityID::Max, direction).into();
            loop {
                let kvs = snapshot
                    .scan_keys(start_key.clone()..end_key.clone(), PAGE_SIZE as u32)
                    .await
                    .map_err(Error::TikvError)?
                    .collect::<Vec<_>>();
                if kvs.is_empty() {
                    break;
                }
                start_key = next_key(&kvs.last().ok_or(Error::NotFound)?.clone());
                let len = kvs.len();

                let mut data_keys = Vec::new();

                for k in kvs {
                    let key = key_to_string(&k)?;
                    let entity_id = EntityID::new_raw(key.split("/").nth(5).ok_or(Error::NotFound)?.to_string());
                    let direction = key.split("/").nth(4).ok_or(Error::NotFound)?.to_string();

                    let data_key = match direction.as_str() {
                        "in" => relation_data_path(T::type_path(), &self_entity_id, &entity_id),
                        "out" => relation_data_path(T::type_path(), &entity_id, &self_entity_id),
                        _ => unreachable!(),
                    };
                    data_keys.push(data_key);
                }

                let data_values = snapshot.batch_get(data_keys).await.map_err(Error::TikvError)?;

                for data in data_values {
                    let key = key_to_string(&data.key())?;
                    let a = EntityID::new_raw(key.split("/").nth(3).ok_or(Error::NotFound)?.to_string());
                    let b = EntityID::new_raw(key.split("/").nth(4).ok_or(Error::NotFound)?.to_string());

                    let entity_id;
                    let direction;

                    if self_entity_id == a {
                        direction = RelationDirection::In;
                        entity_id = b.clone();
                    } else {
                        direction = RelationDirection::Out;
                        entity_id = a.clone();
                    }

                    let value = T::decode(data.value().as_slice()).map_err(Error::DeserializationError)?;
                    yield (entity_id, direction, value);
                }
                if len < PAGE_SIZE {
                    break;
                }
            }
        })
    }

    pub async fn edges_entity<T: KvRelation + prost::Message + Default>(
        &self,
        direction: RelationDirection,
    ) -> Result<Vec<(EntityID, RelationDirection)>, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;
        let edges = self
            .edges_entity_in_txn(T::type_path(), direction, &mut txn)
            .await?;
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(edges)
    }

    pub async fn delete_edges<T: KvRelation + prost::Message + Default>(
        &self,
    ) -> Result<Self, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;
        let edges = self
            .edges_entity_in_txn(T::type_path(), RelationDirection::Both, &mut txn)
            .await?;

        let mut mutations = Vec::new();

        for (entity_id, direction) in edges {
            mutations.extend(vec![
                kvrpcpb::Mutation {
                    key: relation_edge_path(T::type_path(), &self.entity_id, &entity_id, direction)
                        .into(),
                    op: kvrpcpb::Op::Del.into(),
                    ..Default::default()
                },
                kvrpcpb::Mutation {
                    key: relation_edge_path(T::type_path(), &entity_id, &self.entity_id, direction)
                        .into(),
                    op: kvrpcpb::Op::Del.into(),
                    ..Default::default()
                },
            ]);
            match direction {
                RelationDirection::In => {
                    mutations.push(kvrpcpb::Mutation {
                        key: relation_data_path(T::type_path(), &self.entity_id, &entity_id).into(),
                        op: kvrpcpb::Op::Del.into(),
                        ..Default::default()
                    });
                }
                RelationDirection::Out => {
                    mutations.push(kvrpcpb::Mutation {
                        key: relation_data_path(T::type_path(), &entity_id, &self.entity_id).into(),
                        op: kvrpcpb::Op::Del.into(),
                        ..Default::default()
                    });
                }
                RelationDirection::Both => {
                    unreachable!();
                }
            }
        }
        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
    }
}

impl EntityHandler {
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
                .entry(T::type_path().0.to_string())
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
                    value: Into::<String>::into(self.entity_id.clone()).into(),
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

    async fn edges_entity_in_txn(
        &self,
        type_path: TypePath,
        direction: RelationDirection,
        txn: &mut tikv_client::Transaction,
    ) -> Result<Vec<(EntityID, RelationDirection)>, Error> {
        const PAGE_SIZE: usize = 128;
        let mut start_key: Key =
            relation_edge_path(type_path, &self.entity_id, &EntityID::Empty, direction).into();
        let end_key: Key =
            relation_edge_path(type_path, &self.entity_id, &EntityID::Max, direction).into();

        let mut edges = Vec::new();
        loop {
            let kvs = txn
                .scan_keys(start_key.clone()..end_key.clone(), PAGE_SIZE as u32)
                .await
                .map_err(Error::TikvError)?
                .collect::<Vec<_>>();
            if kvs.is_empty() {
                break;
            }
            start_key = next_key(&kvs.last().ok_or(Error::NotFound)?.clone());
            let len = kvs.len();

            for k in kvs {
                let key = key_to_string(&k)?;
                let entity_id =
                    EntityID::new_raw(key.split("/").nth(5).ok_or(Error::NotFound)?.to_string());

                let direction = match key.split("/").nth(4).ok_or(Error::NotFound)? {
                    "in" => RelationDirection::In,
                    "out" => RelationDirection::Out,
                    _ => unreachable!(),
                };
                edges.push((entity_id.clone(), direction));
            }
            if len < PAGE_SIZE {
                break;
            }
        }
        Ok(edges)
    }

    async fn scan_edges_all_in_txn(
        &self,
        txn: &mut tikv_client::Transaction,
    ) -> Result<Vec<(EntityID, RelationDirection, TypePath)>, Error> {
        const PAGE_SIZE: usize = 128;
        let mut start_key: Key = relation_edge_no_type_path(&self.entity_id, TypePath("")).into();
        let end_key: Key = relation_edge_no_type_path(&self.entity_id, TypePath("~")).into();

        let mut edges = Vec::new();
        loop {
            let kvs = txn
                .scan_keys(start_key.clone()..end_key.clone(), PAGE_SIZE as u32)
                .await
                .map_err(Error::TikvError)?
                .collect::<Vec<_>>();

            if kvs.is_empty() {
                break;
            }
            start_key = next_key(&kvs.last().ok_or(Error::NotFound)?.clone());
            let len = kvs.len();

            for k in kvs {
                let key = key_to_string(&k)?;
                let entity_id =
                    EntityID::new_raw(key.split("/").nth(5).ok_or(Error::NotFound)?.to_string());
                let type_path =
                    TypePath(intern_string(key.split("/").nth(3).ok_or(Error::NotFound)?));

                let direction = match key.split("/").nth(4).ok_or(Error::NotFound)? {
                    "in" => RelationDirection::In,
                    "out" => RelationDirection::Out,
                    _ => unreachable!(),
                };
                edges.push((entity_id, direction, type_path));
            }
            if len < PAGE_SIZE {
                break;
            }
        }
        Ok(edges)
    }

    async fn delete_in_txn(
        &self,
        mut txn: &mut tikv_client::Transaction,
        mutations: &mut Vec<kvrpcpb::Mutation>,
    ) -> Result<(), Error> {
        let Some(metadata) = self.get_metadata(&mut txn).await? else {
            txn.rollback().await.map_err(Error::TikvError)?;
            return Err(Error::NotFound);
        };
        for (component_type, component_archetype) in metadata.component_archetypes.iter() {
            let component_type = TypePath(intern_string(component_type.as_str()));
            for (field, key) in component_archetype.index_keys.iter() {
                mutations.push(kvrpcpb::Mutation {
                    key: component_index_path(component_type, field, key, &self.entity_id).into(),
                    op: kvrpcpb::Op::Del.into(),
                    ..Default::default()
                });
            }
            mutations.push(kvrpcpb::Mutation {
                key: component_data_path(component_type, &self.entity_id).into(),
                op: kvrpcpb::Op::Del.into(),
                ..Default::default()
            });
        }
        for (entity_id, direction, type_path) in self.scan_edges_all_in_txn(&mut txn).await? {
            mutations.push(kvrpcpb::Mutation {
                key: relation_edge_path(type_path, &self.entity_id, &entity_id, direction).into(),
                op: kvrpcpb::Op::Del.into(),
                ..Default::default()
            });
            mutations.push(kvrpcpb::Mutation {
                key: relation_edge_path(type_path, &entity_id, &self.entity_id, !direction).into(),
                op: kvrpcpb::Op::Del.into(),
                ..Default::default()
            });
            match direction {
                RelationDirection::In => {
                    mutations.push(kvrpcpb::Mutation {
                        key: relation_data_path(type_path, &self.entity_id, &entity_id).into(),
                        op: kvrpcpb::Op::Del.into(),
                        ..Default::default()
                    });
                }
                RelationDirection::Out => {
                    mutations.push(kvrpcpb::Mutation {
                        key: relation_data_path(type_path, &entity_id, &self.entity_id).into(),
                        op: kvrpcpb::Op::Del.into(),
                        ..Default::default()
                    });
                }
                RelationDirection::Both => {
                    unreachable!();
                }
            }
        }
        self.delete_metadata(&mut txn).await?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct EntityListHandler {
    pub(crate) entity_ids: Vec<EntityID>,
    pub(crate) client: TransactionClient,
}

impl EntityListHandler {
    pub fn new(entity_ids: Vec<EntityID>, client: TransactionClient) -> Self {
        Self { entity_ids, client }
    }

    pub async fn attach(&self, bundle: impl ComponentBundle) -> Result<Self, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;
        let mut mutations = Vec::new();
        for entity_id in self.entity_ids.iter() {
            bundle
                .clone()
                .attach_to(
                    &EntityHandler {
                        entity_id: entity_id.clone(),
                        client: self.client.clone(),
                    },
                    &mut txn,
                    &mut mutations,
                )
                .await?;
        }
        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
    }

    pub async fn get<T: KvComponent + prost::Message + Default>(&self) -> Result<Vec<T>, Error> {
        let mut snapshot = self.client.snapshot(
            self.client
                .current_timestamp()
                .await
                .map_err(Error::TikvError)?,
            tikv_client::TransactionOptions::new_optimistic(),
        );

        snapshot
            .batch_get(
                self.entity_ids
                    .iter()
                    .map(|id| component_data_path(T::type_path(), id))
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(Error::TikvError)?
            .map(|data| T::decode(data.value().as_slice()).map_err(Error::DeserializationError))
            .collect()
    }

    pub async fn delete(&self) -> Result<Self, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;
        let mut mutations = Vec::new();
        for entity_id in self.entity_ids.iter() {
            EntityHandler {
                entity_id: entity_id.clone(),
                client: self.client.clone(),
            }
            .delete_in_txn(&mut txn, &mut mutations)
            .await?;
        }
        txn.batch_mutate(mutations)
            .await
            .map_err(Error::TikvError)?;
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
    }
}
