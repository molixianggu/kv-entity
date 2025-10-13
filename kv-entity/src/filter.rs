use std::marker::PhantomData;

use crate::{DB, KvComponent, db::EntityHandler, error::Error};

pub struct Filter<T> {
    client: DB,
    field_name: String,
    start: Option<String>,
    end: Option<String>,
    value: Option<String>,
    _marker: PhantomData<T>,
}

impl<T> Filter<T>
where
    T: KvComponent + prost::Message + Default,
{
    pub fn new(
        client: DB,
        field_name: String,
        start: Option<String>,
        end: Option<String>,
        value: Option<String>,
    ) -> Self {
        Self {
            client,
            field_name,
            start,
            end,
            value,
            _marker: PhantomData,
        }
    }

    async fn query_entity_id(&self, snapshot: &mut tikv_client::Snapshot) -> Result<String, Error> {
        if let Some(value) = &self.value {
            // 单值查询
            let Some(entity_id) = snapshot
                .get(format!(
                    "index/component/{}/{}/{}",
                    T::type_path(),
                    self.field_name,
                    value
                ))
                .await
                .map_err(Error::TikvError)?
            else {
                return Err(Error::NotFound);
            };
            return Ok(
                String::from_utf8(entity_id).map_err(|e| Error::InvalidEntityId(e.to_string()))?
            );
        } else if let (Some(start), Some(end)) = (&self.start, &self.end) {
            // 范围查询
            let Some(kv) = snapshot
                .scan(
                    format!(
                        "index/component/{}/{}/{}",
                        T::type_path(),
                        self.field_name,
                        start
                    )
                        ..format!(
                            "index/component/{}/{}/{}",
                            T::type_path(),
                            self.field_name,
                            end
                        )
                        .into(),
                    1,
                )
                .await
                .map_err(Error::TikvError)?
                .next()
            else {
                return Err(Error::NotFound);
            };
            return Ok(
                // 转换为字符串
                String::from_utf8(kv.value().to_vec())
                    .map_err(|e| Error::InvalidEntityId(e.to_string()))?,
            );
        } else if let (Some(start), None) = (&self.start, &self.end) {
            // 大于查询
            let Some(kv) = snapshot
                .scan(
                    format!(
                        "index/component/{}/{}/{}",
                        T::type_path(),
                        self.field_name,
                        start
                    )..,
                    1,
                )
                .await
                .map_err(Error::TikvError)?
                .next()
            else {
                return Err(Error::NotFound);
            };
            return Ok(String::from_utf8(kv.value().to_vec())
                .map_err(|e| Error::InvalidEntityId(e.to_string()))?);
        } else if let (None, Some(end)) = (&self.start, &self.end) {
            // 小于查询
            let Some(kv) = snapshot
                .scan(
                    ..format!(
                        "index/component/{}/{}/{}",
                        T::type_path(),
                        self.field_name,
                        end
                    ),
                    1,
                )
                .await
                .map_err(Error::TikvError)?
                .next()
            else {
                return Err(Error::NotFound);
            };
            return Ok(String::from_utf8(kv.value().to_vec())
                .map_err(|e| Error::InvalidEntityId(e.to_string()))?);
        } else {
            return Err(Error::InvalidEntityId("Invalid entity id".to_string()));
        }
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
            .get(format!("component/single/{}/{}", entity_id, T::type_path()))
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
