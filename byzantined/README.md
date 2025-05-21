# RaftSimple with Byzantine Leader Fault

On root:
```sh
go build -o paxos-try
```

Test:

```sh
./maelstrom test -w lin-kv --bin ../RaftSimple/paxos-try --time-limit 10 --concurrency 4 --node-count=2
```

Robust test:

```sh
./maelstrom test -w lin-kv --bin ../RaftSimple/paxos-try --time-limit 60 --node-count 3 --concurrency 10n --rate 100 --nemesis partition --nemesis-interval 3 --test-count 5
```