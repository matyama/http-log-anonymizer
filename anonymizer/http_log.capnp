@0xf42cd342ff520eca;

struct HttpLogRecord {
  timestampEpochMilli @0 :UInt64;
  resourceId @1 :UInt64;
  bytesSent @2 :UInt64;
  requestTimeMilli @3 :UInt64;
  responseStatus @4 :UInt16;
  cacheStatus @5 :Text;
  method @6 :Text;
  remoteAddr @7 :Text;
  url @8 :Text;
}
