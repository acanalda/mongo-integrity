{
  "targets": [
    {
      "target_name": "pingit",
      "sources": [ "lib/pingit.cc", "lib/sync.cc", "lib/async.cc", "lib/pinger.cc" ],
      "include_dirs" : [
          "<!(node -e \"require('nan')\")"
      ]
    }
  ]
}
