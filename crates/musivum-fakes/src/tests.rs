// What the macro generates is tested where it is used, in
// `crates/musivum/src/testutils/fakes_test.rs`. Here: what it says when it cannot make a
// fake, and how much it writes.

use proc_macro2::TokenTree;
use syn::{File, ImplItem, Item, LitStr, Stmt};

use super::*;

fn expanded(attr: &str, item: &str) -> File {
    let tokens = expand(attr.parse().unwrap(), item.parse().unwrap());
    syn::parse2(tokens.clone())
        .unwrap_or_else(|err| panic!("the expansion is not Rust: {err}\n{tokens}"))
}

/// The messages of the `compile_error!`s of an expansion.
fn errors(attr: &str, item: &str) -> Vec<String> {
    expanded(attr, item)
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Macro(item) if item.mac.path.segments.last()?.ident == "compile_error" => {
                Some(item.mac.parse_body::<LitStr>().unwrap().value())
            }
            _ => None,
        })
        .collect()
}

/// The one thing the macro has to say about a trait it cannot fake.
#[track_caller]
fn error(attr: &str, item: &str) -> String {
    let mut errors = errors(attr, item);
    assert_eq!(errors.len(), 1, "{errors:#?}");
    errors.remove(0)
}

/// The names of the methods a test configures the fake with.
fn state_methods(file: &File) -> Vec<String> {
    let mut names = Vec::new();
    for item in &file.items {
        let Item::Const(constant) = item else {
            continue;
        };
        let syn::Expr::Block(block) = &*constant.expr else {
            continue;
        };
        for stmt in &block.block.stmts {
            let Stmt::Item(Item::Impl(item)) = stmt else {
                continue;
            };
            if item.trait_.is_some() {
                continue;
            }
            for item in &item.items {
                if let ImplItem::Fn(function) = item {
                    names.push(function.sig.ident.to_string());
                }
            }
        }
    }
    names
}

fn count_tokens(tokens: TokenStream) -> usize {
    tokens
        .into_iter()
        .map(|tree| match tree {
            TokenTree::Group(group) => 2 + count_tokens(group.stream()),
            _ => 1,
        })
        .sum()
}

#[test]
fn the_methods_of_a_fake() {
    let file = expanded(
        "",
        "pub trait TrackSender: Send + Sync {
            fn id(&self) -> String;
            fn write_rtp(&self, p: &ExtPacket, layer: i32) -> i32;
            fn set_receiver(&self, r: Arc<dyn TrackReceiver>);
            fn close(&self);
            fn r#type(&self) -> u8;
        }",
    );
    assert!(!file.items.iter().any(|item| matches!(item, Item::Macro(_))));
    let names: Vec<&str> = file
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Trait(item) => Some(item.ident.to_string()),
            Item::Type(item) => Some(item.ident.to_string()),
            Item::Struct(item) => Some(item.ident.to_string()),
            _ => None,
        })
        .map(|name| &*name.leak())
        .collect();
    assert_eq!(
        names,
        ["TrackSender", "FakeTrackSender", "FakeTrackSenderState"]
    );
    assert_eq!(
        state_methods(&file),
        [
            // A method that takes nothing has no arguments to ask for.
            "id_call_count",
            "id_calls",
            "id_stub",
            "id_returns",
            "id_returns_with",
            "id_returns_on_call",
            "write_rtp_call_count",
            "write_rtp_calls",
            "write_rtp_stub",
            "write_rtp_args_for_call",
            "write_rtp_returns",
            "write_rtp_returns_with",
            "write_rtp_returns_on_call",
            // A method that returns nothing has nothing to set.
            "set_receiver_call_count",
            "set_receiver_calls",
            "set_receiver_stub",
            "set_receiver_args_for_call",
            "close_call_count",
            "close_calls",
            "close_stub",
            "type_call_count",
            "type_calls",
            "type_stub",
            "type_returns",
            "type_returns_with",
            "type_returns_on_call",
        ]
    );
}

#[test]
fn the_attributes_of_the_macro_are_taken_out_of_the_trait() {
    for (keep, skip) in [
        ("#[fake(keep)]", "#[fake(skip)]"),
        (
            "#[cfg_attr(test, fake(keep))]",
            "#[cfg_attr(test, fake(skip))]",
        ),
    ] {
        let file = expanded(
            "",
            &format!(
                "trait T {{
                    /// Stays.
                    #[must_use]
                    {keep}
                    fn kept(&self) -> u8 {{ 1 }}
                    fn guarded(&self, {skip} #[allow(unused)] guard: &Guard, n: u8);
                }}"
            ),
        );
        let Item::Trait(item) = &file.items[0] else {
            panic!("the trait comes first");
        };
        let text = item.to_token_stream().to_string();
        assert!(!text.contains("fake"), "{text}");
        assert!(
            text.contains("must_use") && text.contains("Stays"),
            "{text}"
        );
        assert!(text.contains("allow (unused)"), "{text}");
        // The kept method is not faked, the parameter is not recorded.
        assert_eq!(
            state_methods(&file),
            [
                "guarded_call_count",
                "guarded_calls",
                "guarded_stub",
                "guarded_args_for_call"
            ]
        );
        let all = file.to_token_stream().to_string();
        assert!(all.contains("NotRecorded , __rt :: rec (& __a1)"), "{all}");
    }
}

#[test]
fn what_is_not_a_trait() {
    assert_eq!(error("", "struct S;"), "`#[fake]` goes on a trait");
    // What it was on is still there.
    assert!(matches!(
        expanded("", "struct S;").items[0],
        Item::Struct(_)
    ));
}

#[test]
fn traits_that_cannot_be_faked() {
    assert_eq!(
        error("", "trait T<P> { fn a(&self, p: &P); }"),
        "`#[fake]` does not support a trait with generic parameters: the choices a fake makes \
         by type (what is recorded, the zero value) need concrete types"
    );
    assert_eq!(
        error("", "trait T where Self: Send { fn a(&self); }"),
        "`#[fake]` does not support a `where` clause on the trait: write the bounds as \
         supertraits"
    );
    assert_eq!(
        error("", "unsafe trait T { fn a(&self); }"),
        "`#[fake]` does not support an `unsafe` trait"
    );
    assert_eq!(
        error("", "trait T { type Item; fn a(&self) -> u8; }"),
        "`#[fake]` does not support associated types: the fake could not choose a type for \
         `Item`"
    );
    assert_eq!(
        error("", "trait T { const N: usize; fn a(&self); }"),
        "`#[fake]` does not support an associated constant without a default: the fake could \
         not choose a value for `N`"
    );
    assert_eq!(
        error("", "trait T { some_macro!(); }"),
        "`#[fake]` cannot look into this item: only methods and associated constants with a \
         default can be in a faked trait"
    );
    // The trait is kept, so that the rest of the crate is not full of errors about it.
    let file = expanded("", "trait T<P> { fn a(&self, p: &P); }");
    assert!(matches!(&file.items[0], Item::Trait(item) if item.ident == "T"));
    assert_eq!(file.items.len(), 2);
}

#[test]
fn methods_that_cannot_be_faked() {
    let keep = "give it a default body and mark it `#[cfg_attr(test, fake(keep))]` \
                (`#[fake(keep)]` where the macro is not behind `cfg_attr`): the fake then has \
                the default body";
    assert_eq!(
        error("", "trait T { fn new() -> u8; }"),
        format!(
            "`#[fake]` cannot fake `new`: it has no `self`, so there is no fake to record the \
             call in; {keep}"
        )
    );
    let generic = format!(
        "`#[fake]` cannot fake the generic method `get`: a stub and the recorded arguments \
         need concrete types. Take a `Box<dyn ..>`, an `Arc<dyn ..>` or a `&dyn ..` instead, \
         or {keep}"
    );
    assert_eq!(
        error("", "trait T { fn get<V: Into<u8>>(&self, v: V); }"),
        generic
    );
    assert_eq!(
        error("", "trait T { fn get<const N: usize>(&self, v: [u8; N]); }"),
        generic
    );
    assert_eq!(
        error("", "trait T { fn on_close(&self, f: impl Fn() + Send); }"),
        format!(
            "`#[fake]` cannot fake `on_close`: `impl Trait` makes it a generic method, and a \
             stub and the recorded arguments need concrete types. Take a `Box<dyn ..>`, an \
             `Arc<dyn ..>` or a `&dyn ..` instead, or {keep}"
        )
    );
    assert_eq!(
        error("", "trait T { fn all(&self) -> impl Iterator<Item = u8>; }"),
        format!(
            "`#[fake]` cannot fake `all`: the return type `impl Trait` has no name, so the \
             fake cannot keep a value of it. Return a `Box<dyn ..>`, or {keep}"
        )
    );
    assert_eq!(
        error("", "trait T { fn same(&self, other: &Self) -> bool; }"),
        format!(
            "`#[fake]` cannot fake `same`: `Self` in a parameter would be the fake itself; \
             {keep}"
        )
    );
    assert_eq!(
        error("", "trait T { fn copy(&self) -> Box<Self>; }"),
        format!(
            "`#[fake]` cannot fake `copy`: `Self` in the return type would be the fake \
             itself; {keep}"
        )
    );
    assert_eq!(
        error("", "trait T { fn stop(&self) -> !; }"),
        format!(
            "`#[fake]` cannot fake `stop`: a method that never returns has nothing a fake \
             could return; {keep}"
        )
    );
    assert_eq!(
        error(
            "",
            "trait T { fn pick<'a, 'b: 'a>(&self, a: &'a str, b: &'b str) -> &'a str; }"
        ),
        format!(
            "`#[fake]` cannot fake `pick`: a bound on a lifetime parameter cannot be written \
             for a stub; {keep}"
        )
    );
    let parameters: Vec<String> = (0..13).map(|i| format!("p{i}: u8")).collect();
    assert_eq!(
        error(
            "",
            &format!("trait T {{ fn wide(&self, {}); }}", parameters.join(", "))
        ),
        format!("`#[fake]` cannot fake `wide`: it has more than 12 parameters; {keep}")
    );
    // Each of them is fine with its body kept.
    let file = expanded(
        "",
        "trait T {
            #[fake(keep)]
            fn new() -> u8 { 1 }
            #[fake(keep)]
            fn get<V: Into<u8>>(&self, v: V) -> u8 { v.into() }
            #[fake(keep)]
            fn same(&self, other: &Self) -> bool { true }
            fn a(&self);
        }",
    );
    assert_eq!(state_methods(&file), ["a_call_count", "a_calls", "a_stub"]);
    // Every method that cannot be faked is named, not only the first.
    assert_eq!(
        errors(
            "",
            "trait T { fn new() -> u8; fn copy(&self) -> Box<Self>; type Item; }"
        )
        .len(),
        3
    );
}

#[test]
fn names_that_run_into_each_other() {
    assert_eq!(
        error(
            "",
            "trait T { fn close(&self); fn close_calls(&self) -> u8; }"
        ),
        "the fake of `close` gets a method `close_calls`, and the trait has a method of that \
         name too: one of the two has to be renamed"
    );
}

#[test]
fn what_is_written_wrongly() {
    assert_eq!(
        error("extend(A)", "trait T: A { fn a(&self); }"),
        "unknown option `extend`: `#[fake]` takes `extends(SuperTrait, ..)`, the supertraits \
         whose fakes the fake embeds"
    );
    assert_eq!(
        error("extends(B)", "trait T: A { fn a(&self); }"),
        "`B` is not a supertrait of `T`: `extends(..)` names supertraits, as they are written \
         after the colon"
    );
    assert_eq!(
        error("", "trait T { #[fake(skip)] fn a(&self); }"),
        "unknown `fake(skip)` on a method: a method takes `fake(keep)`, a parameter \
         `fake(skip)`"
    );
    assert_eq!(
        error("", "trait T { fn a(&self, #[fake(keep)] b: u8); }"),
        "unknown `fake(keep)` on a parameter: a parameter other than `self` takes \
         `fake(skip)`, a method `fake(keep)`"
    );
    assert_eq!(
        error("", "trait T { #[fake(keep)] fn a(&self); }"),
        "`fake(keep)` keeps the default body of a method, and `a` has none"
    );
    assert_eq!(
        error(
            "",
            "trait T { fn a(&self, #[cfg_attr(test, fake(skip), allow(unused))] b: u8); }"
        ),
        "`fake(..)` has to be in a `cfg_attr` of its own: `#[cfg_attr(test, fake(..))]`"
    );
}

#[test]
fn the_supertraits_whose_fakes_are_embedded() {
    let embedded = |attr: &str, supertraits: &str| -> Vec<String> {
        let file = expanded(attr, &format!("trait T: {supertraits} {{ fn a(&self); }}"));
        let Some(Item::Struct(state)) = file.items.get(2) else {
            panic!("the state is the third item");
        };
        state
            .fields
            .iter()
            .filter(|field| {
                field
                    .ident
                    .as_ref()
                    .unwrap()
                    .to_string()
                    .starts_with("__super")
            })
            .map(|field| field.ty.to_token_stream().to_string())
            .collect()
    };
    let state =
        |path: &str| format!("< dyn {path} as crate :: testutils :: fakes :: Faked > :: State");

    assert!(embedded("", "Send + Sync + 'static + Sized + Unpin").is_empty());
    assert!(
        embedded(
            "",
            "std::fmt::Debug + Any + core::fmt::Display + ::std::any::Any"
        )
        .is_empty()
    );
    assert_eq!(
        embedded("", "Participant + Send + types::Listener + Debug"),
        [state("Participant"), state("types :: Listener")]
    );
    assert_eq!(
        embedded(
            "extends(types::Listener)",
            "Participant + Send + types::Listener"
        ),
        [state("types :: Listener")]
    );
    assert!(embedded("extends()", "Participant + Display").is_empty());
}

#[test]
fn how_much_is_written_for_a_method() {
    let shapes = [
        "fn m{i}(&self) -> String;",
        "fn m{i}(&self, update: &Message, from_admin: bool) -> Result<(), Error>;",
        "fn m{i}(&self, other: Arc<dyn Other>, reason: Reason);",
        "fn m{i}(&self, data: &[u8], kind: i32, ids: Vec<String>) -> Result<Message, Error>;",
        "fn m{i}(&self);",
    ];
    let with_methods = |n: usize| -> usize {
        let methods: String = (0..n)
            .map(|i| shapes[i % shapes.len()].replace("{i}", &i.to_string()))
            .collect();
        count_tokens(expand(
            TokenStream::new(),
            format!("pub trait Big: Send + Sync {{ {methods} }}")
                .parse()
                .unwrap(),
        ))
    };
    let fixed = with_methods(0);
    let per_method = (with_methods(150) - fixed) / 150;
    println!("{fixed} tokens for a trait, {per_method} more for a method");
    // A method with arguments and a result gets a field, its implementation and seven
    // one-line methods to configure it: some 550 tokens, 40 lines if they were formatted (a
    // method of a counterfeiter fake is about 50 lines of Go). Over these shapes it is 470.
    // Not a limit that means anything by itself: it is here so that a change that makes
    // every method of every fake much bigger is noticed.
    assert!(per_method < 520, "{per_method} tokens for a method");
}
