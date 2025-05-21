# RaftSimple

Comandos (Apenas notas minhas)

```sh
cd ../TF/RaftTry; go build; cd ../../maelstrom
```

Simple test:

```sh
./maelstrom test -w lin-kv --bin ../TF/RaftTry/paxos-try --time-limit 10 --concurrency 4 --node-count=2
```

Robust test:

```sh
./maelstrom test -w lin-kv --bin ../TF/RaftTry/paxos-try --time-limit 60 --node-count 3 --concurrency 10n --rate 100 --nemesis partition --nemesis-interval 3 --test-count 5
```