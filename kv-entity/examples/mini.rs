use futures::StreamExt;
use kv_entity::DB;
use kv_entity::Error;

#[derive(kv_entity::KvComponent, Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UserInfo {
    #[index]
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,

    #[index]
    #[prost(int32, tag = "2")]
    pub age: i32,

    #[prost(string, tag = "3")]
    pub email: ::prost::alloc::string::String,
}

#[derive(kv_entity::KvComponent, Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UserExtend {
    #[prost(string, tag = "1")]
    pub extend: ::prost::alloc::string::String,
}

#[derive(kv_entity::KvRelation)]
#[derive(kv_entity::KvComponent, Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct FriendRelation {
    #[prost(int32, tag = "1")]
    pub favorability: i32,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::Builder::from_default_env()
        .filter_module("mini", log::LevelFilter::Debug)
        .filter_module("kv_entity", log::LevelFilter::Debug)
        .init();

    let db = DB::new(vec!["172.20.8.107:2379".to_string()]).await?;

    for i in 0..10 {
        let uid = uuid::Uuid::new_v4().to_string();
        db.entity(&uid)
            .attach(UserInfo {
                name: "Bob".to_string(),
                age: i,
                email: "bob@example.com".to_string(),
            })
            .await?;
    }

    let mut iterator = db.iterator::<UserInfo>();
    while let Some(Ok((entity_id, data))) = iterator.next().await {
        log::info!("entity_id = {:?}, data = {:?}", entity_id, data);
        db.entity(&entity_id).delete().await?;
    }

    let uid = uuid::Uuid::new_v4().to_string();
    db.entity(&uid)
        .attach((
            UserInfo {
                name: "Alice".to_string(),
                age: 25,
                email: "alice@example.com".to_string(),
            },
            UserExtend {
                extend: "extend".to_string(),
            },
        ))
        .await?;
    log::info!("attach entity {} success", uid);

    let a = db.query::<UserInfo>().name("Alice").single().await?;
    log::info!("a = {:?}", a);

    let b = db
        .query::<UserInfo>()
        .age(25)
        .entity()
        .await?
        .get::<UserExtend>()
        .await?
        .unwrap();
    log::info!("b = {:?}", b);

    db.entity(&uid).delete().await?;

    log::info!("delete entity {} success", uid);

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    Ok(())
}
