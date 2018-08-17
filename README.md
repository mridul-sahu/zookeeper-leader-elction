# zookeeper-leader-election
A small demonstration of using Zookeeper leader election in Golang.

## Overview
This is a demonstration of using zookeeper leader election to choose a leader to perform some task. Here we have a microservice
that listens and publishes to a **Redis PubSub channel**, these messages have to be stored in a **Mongo DB** whose schema is
```
type electionMessages struct {
	Topic    string   `bson:"topic"`
	Messages []string `bson:"messages"`
}
```
In this to update the list of messages we copy and rewrite the list with extra messages. As you can see if two people try to update
the list at the same time, some data will be lost, so only one person should write the messages to the database at a time.

In **Kubernetes** you can have many replicas of some pod running at a time, so here we use the **Zookeeper's Leader Election**
mechanism to elect a leader among these replicas and only the leader will write to the database.

All replicas of the golang microservice publish their messages to a Redis PubSub channel and only the leader subscribe to it and writes.

## TODO
- Complete the tests
- Add create topic functionality, as many topics can be handled simultaneously, i.e
  each topic will have its own election, channel and leader.
  
**Note: This is just a demonstration so this repo might not get more updates, PRs are always welcomed though.**  