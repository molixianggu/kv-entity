use crate::{KvComponent, entity_handler::EntityHandler, error::Error, meta::EntityMetadata};
use prost::Message;
use tikv_client::proto::kvrpcpb;

pub trait ComponentBundle: Sized {
    fn attach_to(
        self,
        entity: &EntityHandler,
        txn: &mut tikv_client::Transaction,
        mutations: &mut Vec<kvrpcpb::Mutation>,
    ) -> impl std::future::Future<Output = Result<(), Error>> + Send;

    fn clone(&self) -> Self;
}

impl<T> ComponentBundle for T
where
    T: KvComponent + Message + Default + Clone,
{
    async fn attach_to(
        self,
        entity: &EntityHandler,
        txn: &mut tikv_client::Transaction,
        mutations: &mut Vec<kvrpcpb::Mutation>,
    ) -> Result<(), Error> {
        let mut metadata = entity
            .get_metadata(txn)
            .await?
            .unwrap_or(EntityMetadata::default());
        entity
            .attach_component_in_txn(mutations, &mut metadata, self)
            .await?;

        entity.update_metadata(txn, metadata).await?;
        Ok(())
    }

    fn clone(&self) -> Self {
        Clone::clone(self)
    }
}

macro_rules! impl_component_bundle_for_tuple {
    ($($T:ident),+) => {
        impl<$($T),+> ComponentBundle for ($($T,)+)
        where
            $($T: KvComponent + Message + Default + Clone, )+
        {
            async fn attach_to(self, entity: &EntityHandler, txn: &mut tikv_client::Transaction, mutations: &mut Vec<kvrpcpb::Mutation>) -> Result<(), Error> {
                let mut metadata = entity
                    .get_metadata(txn)
                    .await?
                    .unwrap_or(EntityMetadata::default());

                #[allow(non_snake_case)]
                let ($($T,)+) = self;
                $(
                    entity.attach_component_in_txn(mutations, &mut metadata, $T).await?;
                )+

                entity.update_metadata(txn, metadata).await?;
                Ok(())
            }
            fn clone(&self) -> Self {
                Clone::clone(self)
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
