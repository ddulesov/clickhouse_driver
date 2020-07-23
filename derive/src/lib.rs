extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate syn;

use proc_macro::TokenStream;

use syn::export::TokenStream2;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(IsCommand)]
pub fn command_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let gen = command_impl(&ast);
    gen.into()
}

fn command_impl(ast: &syn::DeriveInput) -> TokenStream2 {
    let gen = &ast.generics;
    let name = &ast.ident;
    quote! {
        impl#gen Command for #name#gen {

        }
    }
}
