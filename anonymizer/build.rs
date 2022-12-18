fn main() {
    ::capnpc::CompilerCommand::new()
        //.src_prefix("..")
        //.file("../http_log.capnp")
        // NOTE: keeping a copy here makes Docker build easier
        .file("http_log.capnp")
        .run()
        .expect("compiling schema");
}
