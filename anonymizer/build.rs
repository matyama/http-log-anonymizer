fn main() {
    ::capnpc::CompilerCommand::new()
        .src_prefix("..")
        .file("../http_log.capnp")
        .run()
        .expect("compiling schema");
}
