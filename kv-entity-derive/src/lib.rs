use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, Type, parse_macro_input};

#[proc_macro_derive(KvComponent, attributes(index))]
pub fn derive_kv_components(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let struct_name = &input.ident;
    let query_struct_name = format_ident!("__{}Query__", struct_name);

    // 解析结构体字段，找出带有 #[index] 属性的字段
    let indexed_fields = match &input.data {
        Data::Struct(data_struct) => {
            match &data_struct.fields {
                Fields::Named(fields) => {
                    fields.named.iter()
                        .filter_map(|field| {
                            // 检查字段是否有 #[index] 属性
                            let has_index = field.attrs.iter()
                                .any(|attr| attr.path().is_ident("index"));
                            
                            if has_index {
                                let field_name = field.ident.as_ref()?;
                                let field_type = &field.ty;
                                Some((field_name, field_type))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                }
                _ => vec![],
            }
        }
        _ => vec![],
    };

    let indexed_field_names: Vec<_> = indexed_fields.iter().map(|(field_name, _)| {
        let name_str = field_name.to_string();
        quote! { #name_str }
    }).collect();

    // 为每个索引字段生成编码函数
    let encode_functions = indexed_fields.iter().map(|(field_name, field_type)| {
        let encode_fn_name = format_ident!("encode_{}", field_name);
        let is_string_type = quote!(#field_type).to_string().contains("String");
        let is_numeric_type = matches_numeric_type(field_type);
        
        if is_string_type {
            quote! {
                pub fn #encode_fn_name(value: impl Into<String>) -> String {
                    value.into()
                }
            }
        } else if is_numeric_type {
            let type_str = quote!(#field_type).to_string();
            let encoding_logic = generate_numeric_encoding(&type_str);
            quote! {
                pub fn #encode_fn_name(value: #field_type) -> String {
                    #encoding_logic
                }
            }
        } else {
            quote! {}
        }
    });
    
    // 为每个索引字段生成查询方法
    let query_methods = indexed_fields.iter().map(|(field_name, field_type)| {
        let method_name = format_ident!("{}", field_name);
        
        // 判断字段类型，生成不同的方法签名
        let is_string_type = quote!(#field_type).to_string().contains("String");
        let is_numeric_type = matches_numeric_type(field_type);
        
        let encode_fn_name = format_ident!("encode_{}", field_name);
        
        let field_name_str = field_name.to_string();
        
        if is_string_type {
            // 字符串类型，接受 impl Into<String>
            quote! {
                pub fn #method_name(&mut self, value: impl Into<String>) -> kv_entity::Filter<#struct_name> {
                    let encoded = #struct_name::#encode_fn_name(value);
                    kv_entity::Filter::new(self.client.clone(), #field_name_str.to_string(), None, None, Some(encoded))
                }
            }
        } else if is_numeric_type {
            // 数字类型 - 调用对应的编码函数
            quote! {
                pub fn #method_name(&mut self, value: #field_type) -> kv_entity::Filter<#struct_name> {
                    let encoded = #struct_name::#encode_fn_name(value);
                    kv_entity::Filter::new(self.client.clone(), #field_name_str.to_string(), None, None, Some(encoded))
                }
            }
        } else {
            // 其他类型不支持索引，产生编译错误
            let error_msg = format!(
                "Field '{}' has type '{}' which is not supported for indexing. Only String and numeric types are supported.",
                field_name, quote!(#field_type)
            );
            quote! {
                compile_error!(#error_msg);
            }
        }
    });

    // 生成 indexed_fields 方法的代码
    let indexed_fields_impl = {
        let field_encodings = indexed_fields.iter().map(|(field_name, field_type)| {
            let field_name_str = field_name.to_string();
            let is_string_type = quote!(#field_type).to_string().contains("String");
            let is_numeric_type = matches_numeric_type(field_type);
            
            let encode_fn_name = format_ident!("encode_{}", field_name);
            
            if is_string_type {
                quote! {
                    result.push((#field_name_str.to_string(), #struct_name::#encode_fn_name(self.#field_name.clone())));
                }
            } else if is_numeric_type {
                quote! {
                    result.push((#field_name_str.to_string(), #struct_name::#encode_fn_name(self.#field_name)));
                }
            } else {
                quote! {}
            }
        });
        
        quote! {
            fn indexed_fields(&self) -> Vec<(String, String)> {
                let mut result = Vec::new();
                #(#field_encodings)*
                result
            }

            fn indexed_field_names() -> Vec<&'static str> {
                vec![#(#indexed_field_names),*]
            }
        }
    };

    let expanded = quote! {
        impl kv_entity::KvComponent for #struct_name {
            type Query = #query_struct_name;

            fn type_path() -> &'static str {
                concat!(module_path!(), "::", stringify!(#struct_name))
            }

            fn query(client: kv_entity::DB) -> #query_struct_name {
                #query_struct_name { client }
            }
            
            #indexed_fields_impl
        }

        impl #struct_name {
            #(#encode_functions)*
        }

        pub struct #query_struct_name {
            pub client: kv_entity::DB,
        }

        impl #query_struct_name {
            #(#query_methods)*
        }

        inventory::submit! {
            kv_entity::ComponentMeta {
                type_path: concat!(module_path!(), "::", stringify!(#struct_name)),
                indexed_field_names: || vec![#(#indexed_field_names),*],
            }
        }
    };

    TokenStream::from(expanded)
}

// 生成数字类型的编码逻辑
fn generate_numeric_encoding(type_str: &str) -> proc_macro2::TokenStream {
    match type_str {
        // 无符号整数 - 直接零填充
        "u8" => quote! { format!("{:03}", value) },
        "u16" => quote! { format!("{:05}", value) },
        "u32" => quote! { format!("{:010}", value) },
        "u64" => quote! { format!("{:020}", value) },
        "u128" => quote! { format!("{:039}", value) },
        "usize" => quote! { format!("{:020}", value) }, // 假设最大为 u64
        
        // 有符号整数 - 偏移后零填充（让负数也能正确排序）
        "i8" => quote! { 
            let offset_value = (value as i16 + 128) as u16;
            format!("{:03}", offset_value)
        },
        "i16" => quote! {
            let offset_value = (value as i32 + 32768) as u32;
            format!("{:05}", offset_value)
        },
        "i32" => quote! {
            let offset_value = (value as i64 + 2147483648) as u64;
            format!("{:010}", offset_value)
        },
        "i64" => quote! {
            let offset_value = (value as i128 + 9223372036854775808) as u128;
            format!("{:020}", offset_value)
        },
        "i128" => quote! {
            // i128 需要特殊处理，使用大数偏移
            let offset_value = value.wrapping_add(170141183460469231731687303715884105728u128 as i128) as u128;
            format!("{:039}", offset_value)
        },
        "isize" => quote! {
            // 假设 isize 最大为 i64
            let offset_value = (value as i128 + 9223372036854775808) as u128;
            format!("{:020}", offset_value)
        },
        
        // 浮点数 - 转换为可排序的二进制表示
        "f32" => quote! {
            let bits = value.to_bits();
            // 如果是负数，翻转所有位；如果是正数，只翻转符号位
            let sortable_bits = if value.is_sign_negative() {
                !bits
            } else {
                bits ^ (1u32 << 31)
            };
            format!("{:010}", sortable_bits)
        },
        "f64" => quote! {
            let bits = value.to_bits();
            // 如果是负数，翻转所有位；如果是正数，只翻转符号位
            let sortable_bits = if value.is_sign_negative() {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            format!("{:020}", sortable_bits)
        },
        
        _ => quote! { format!("{:020}", value) }, // 默认处理
    }
}

// 辅助函数：判断类型是否为数字类型
fn matches_numeric_type(ty: &Type) -> bool {
    let type_str = quote!(#ty).to_string();
    matches!(
        type_str.as_str(),
        "i8" | "i16" | "i32" | "i64" | "i128" | "isize" |
        "u8" | "u16" | "u32" | "u64" | "u128" | "usize" |
        "f32" | "f64"
    )
}
