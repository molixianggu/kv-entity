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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let db = DB::new(vec!["172.20.8.107:2379".to_string()]).await?;

    db.drop_all().await?;

    db.entity("1")
        .await
        .attach(UserInfo {
            name: "Alice".to_string(),
            age: 25,
            email: "alice@example.com".to_string(),
        })
        .await?
        .attach(UserExtend {
            extend: "extend".to_string(),
        })
        .await?;

    let a = db.query::<UserInfo>().name("Alice").single().await?;
    println!("{:?}", a);

    let b = db
        .query::<UserInfo>()
        .age(25)
        .entity()
        .await?
        .get::<UserExtend>()
        .await?
        .unwrap();
    println!("{:?}", b);

    db.entity("1").await.delete().await?;

    Ok(())
}
