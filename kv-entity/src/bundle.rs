use crate::{KvComponent, db::EntityHandler, error::Error, meta::EntityMetadata};
use prost::Message;

pub trait ComponentBundle: Sized {
    fn attach_to(
        self,
        entity: &EntityHandler,
    ) -> impl std::future::Future<Output = Result<EntityHandler, Error>> + Send;
}

impl<T> ComponentBundle for T
where
    T: KvComponent + Message + Default,
{
    async fn attach_to(self, entity: &EntityHandler) -> Result<EntityHandler, Error> {
        let mut txn = entity
            .client
            .begin_optimistic()
            .await
            .map_err(Error::TikvError)?;

        let mut metadata = entity
            .get_metadata(&mut txn)
            .await?
            .unwrap_or(EntityMetadata::default());
        let mut mutations = Vec::new();
        entity
            .attach_component_in_txn(&mut mutations, &mut metadata, self)
            .await?;
        if !mutations.is_empty() {
            txn.batch_mutate(mutations)
                .await
                .map_err(Error::TikvError)?;
        }
        entity.update_metadata(&mut txn, metadata).await?;
        txn.commit().await.map_err(Error::TikvError)?;
        Ok(entity.clone())
    }
}

macro_rules! impl_component_bundle_for_tuple {
    ($($T:ident),+) => {
        impl<$($T),+> ComponentBundle for ($($T,)+)
        where
            $($T: KvComponent + Message + Default,)+
        {
            async fn attach_to(self, entity: &EntityHandler) -> Result<EntityHandler, Error> {
                let mut txn = entity
                    .client
                    .begin_optimistic()
                    .await
                    .map_err(Error::TikvError)?;


                let mut metadata = entity
                    .get_metadata(&mut txn)
                    .await?
                    .unwrap_or(EntityMetadata::default());
                let mut mutations = Vec::new();

                #[allow(non_snake_case)]
                let ($($T,)+) = self;
                $(
                    entity.attach_component_in_txn(&mut mutations, &mut metadata, $T).await?;
                )+

                if !mutations.is_empty() {
                    txn.batch_mutate(mutations)
                        .await
                        .map_err(Error::TikvError)?;
                }
                entity.update_metadata(&mut txn, metadata).await?;
                txn.commit().await.map_err(Error::TikvError)?;
                Ok(entity.clone())
            }
        }
    };
}

impl_component_bundle_for_tuple!(T1);
impl_component_bundle_for_tuple!(T1, T2);
impl_component_bundle_for_tuple!(T1, T2, T3);
impl_component_bundle_for_tuple!(T1, T2, T3, T4);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5, T6);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5, T6, T7);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_component_bundle_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
