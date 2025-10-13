use tikv_client::TransactionClient;

use crate::{KvComponent, all_components, error::Error};

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

    pub async fn entity(&self, entity_id: impl Into<String>) -> EntityHandler {
        EntityHandler {
            entity_id: format!("e-{}", entity_id.into()),
            client: self.client.clone(),
        }
    }

    pub async fn resource(&self) -> EntityHandler {
        EntityHandler {
            entity_id: "resource".to_string(),
            client: self.client.clone(),
        }
    }

    pub fn query<T: KvComponent + prost::Message + Default>(&self) -> T::Query {
        T::query(self.clone())
    }

    pub async fn drop_all(&self) -> Result<Self, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        for key in txn
            .scan_keys("".to_string().., 10000)
            .await
            .map_err(Error::TikvError)?
        {
            txn.delete(key).await.map_err(Error::TikvError)?;
        }

        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
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
            .get(format!(
                "component/single/{}/{}",
                self.entity_id,
                T::type_path()
            ))
            .await
            .map_err(Error::TikvError)?
        else {
            return Ok(None);
        };

        let message = T::decode(data.as_slice()).map_err(Error::DeserializationError)?;

        Ok(Some(message))
    }

    pub async fn attach<T: KvComponent + prost::Message + Default>(
        &self,
        value: T,
    ) -> Result<Self, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        // 写入索引
        {
            let old_data = txn
                .get(format!(
                    "component/single/{}/{}",
                    self.entity_id,
                    T::type_path()
                ))
                .await
                .map_err(Error::TikvError)?;

            if let Some(old_data) = old_data {
                let old_value =
                    T::decode(old_data.as_slice()).map_err(Error::DeserializationError)?;
                for (n, v) in old_value.indexed_fields() {
                    txn.delete(format!("index/component/{}/{}/{}", T::type_path(), n, v))
                        .await
                        .map_err(Error::TikvError)?;
                }
            }

            for (n, v) in value.indexed_fields() {
                txn.put(
                    format!("index/component/{}/{}/{}", T::type_path(), n, v),
                    self.entity_id.clone(),
                )
                .await
                .map_err(Error::TikvError)?;
            }
        }

        let data = value.encode_to_vec();

        txn.put(
            format!("component/single/{}/{}", self.entity_id, T::type_path()),
            data,
        )
        .await
        .map_err(Error::TikvError)?;

        txn.commit().await.map_err(Error::TikvError)?;

        Ok(self.clone())
    }

    pub async fn detach<T: KvComponent + prost::Message + Default>(&self) -> Result<Self, Error> {
        let mut txn = self
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        if !T::indexed_field_names().is_empty() {
            let Some(data) = txn
                .get(format!(
                    "component/single/{}/{}",
                    self.entity_id,
                    T::type_path()
                ))
                .await
                .map_err(Error::TikvError)?
            else {
                return Ok(self.clone());
            };

            let value = T::decode(data.as_slice()).map_err(Error::DeserializationError)?;
            for (n, v) in value.indexed_fields() {
                txn.delete(format!("index/component/{}/{}/{}", T::type_path(), n, v))
                    .await
                    .map_err(Error::TikvError)?;
            }
        }

        txn.delete(format!(
            "component/single/{}/{}",
            self.entity_id,
            T::type_path()
        ))
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

        let all_components = all_components();
        for key in txn
            .scan_keys(format!("component/single/{}/", self.entity_id)..format!("component/single/{}/~", self.entity_id).into(), 10000)
            .await
            .map_err(Error::TikvError)?
        {
            let key = String::from_utf8(Into::<Vec<u8>>::into(key)).map_err(Error::InvalidUtf8)?;
            println!("删除组件: {:?}", key);
            let Some(type_path) = key.split("/").nth(3) else {
                continue;
            };
            println!("删除类型: {}", type_path);
            let Some(indexed_field_names) = all_components.get(type_path) else {
                continue;
            };
            if indexed_field_names.is_empty() {
                continue;
            }
            for field_name in indexed_field_names {
                let Some(data) = txn.get(key.clone()).await.map_err(Error::TikvError)? else {
                    continue;
                };
                println!("删除索引: {:?} {} {:?}", key, field_name, data);
            }
        }
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(self.clone())
    }
}
