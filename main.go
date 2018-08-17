package main

import (
	"log"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"github.com/Comcast/go-leaderelection"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"encoding/json"
	"time"
)

const (
	TIMEOUT        = 10
	DB             = "test"
	COLLECTION     = "messages"
	ELECTION_TOPIC = "/topic"
)

type electionMessages struct {
	Topic    string   `bson:"topic"`
	Messages []string `bson:"messages"`
}

type server struct {
	redisPubSubConn *redis.PubSubConn
	candidate       *leaderelection.Election
	pubSubClosed    chan bool
}

/*
 * Ensure Election Resource makes sure that a election resource named 'ELECTION_TOPIC' exists
 * It takes a zookeeper connection and returns an error which is nil if it doesn't encounter problems
 */
func ensureElectionResource(zkConn *zk.Conn) error {
	// Check if election resource exists
	exists, _, err := zkConn.Exists(ELECTION_TOPIC)
	if err != nil {
		log.Printf("Error validating electionResource (%v): %v", ELECTION_TOPIC, err)
		return err
	}

	if !exists {
		log.Printf("Creating election node")
		// Create the election node in ZooKeeper
		_, err = zkConn.Create(ELECTION_TOPIC, []byte(""), 0, zk.WorldACL(zk.PermAll))

		if err != nil {
			log.Printf("Error creating the election node (%s): %v", ELECTION_TOPIC, err)
			return err
		}
	}

	return nil
}

/*
 * Write to DB finds the entry having topic 'ELECTION_TOPIC' and write the message to its
 * list of messages. This needs to be done by only leader as it will lead to inconsistencies
 * if done concurrently. It returns an error which is nil if it succeeds.
 */
func writeToDB(message string) error {
	log.Printf("connecting to database")

	session, err := mgo.Dial(os.Getenv("DB_URI"))

	if err != nil {
		log.Printf("Could not connect to DB : %v", err)
		return err
	}

	defer session.Close()

	collection := session.DB(DB).C(COLLECTION)

	var em electionMessages
	em.Topic = ELECTION_TOPIC

	err = collection.Find(bson.M{"topic": em.Topic}).One(&em)

	if err != nil {
		log.Printf("topic not found")
	}

	em.Messages = append(em.Messages, message)

	_, err = collection.Upsert(
		bson.M{"topic": em.Topic},
		bson.M{"$set": bson.M{"messages": em.Messages}},
	)

	if err != nil {
		log.Printf("Insert Failed")
		return err
	}

	return nil
}

/*
 * Process Redis PubSub listens to the server's redisPubSubConn and writes to DB and message it receives
 * It also manages server's pubSubClosed channel if it encounters any error.
 */
func (s *server) processRedisPubSub() {
	for {
		switch v := s.redisPubSubConn.Receive().(type) {
		case redis.Message:

			var message string

			err := json.Unmarshal(v.Data, &message)

			if err != nil {
				log.Printf("Could not Unmarshal")
				s.pubSubClosed <- true
				return
			}
			// Write message to db
			err = writeToDB(message)

			if err != nil {
				s.pubSubClosed <- true
				return
			}

		case redis.Subscription:
			log.Printf("subscription message: %s: %s %d\n", v.Channel, v.Kind, v.Count)

		case error:
			log.Printf("error pub/sub, delivery has stopped: %v", v)
			s.pubSubClosed <- true
			return
		}
	}
}

/*
 * Monitor Leadership registers this instance as a candidate for becoming a leader.
 * It also listens to the election status and returns if an error is encountered.
 * It also returns if the server's pubSubConnection channel us closed.
 * Only leader should subscribe to the Redis PubSub channel named ELECTION_TOPIC,
 * thus only it can write to DB and maintain consistency
 */
func (s *server) monitorLeadership() {
	go s.candidate.ElectLeader()

	log.Printf("Monitoring leadership status")

	for {
		select {
		case status, ok := <-s.candidate.Status():
			if !ok {
				log.Printf("Channel closed, election is terminated")
				s.candidate.Resign()
				return
			}

			if status.Err != nil {
				log.Printf("Received election status error: %v for candidate %v", status.Err, status.CandidateID)
				s.candidate.Resign()
				return
			}

			log.Printf("Candidate received status message: %v", status)

			if status.Role == leaderelection.Leader {
				// Subscribe to channel
				s.redisPubSubConn.Subscribe(ELECTION_TOPIC)
			} else {
				// Unsubscribe to channel
				s.redisPubSubConn.Unsubscribe(ELECTION_TOPIC)
			}

		case <-s.pubSubClosed:
			log.Printf("Redis Pub Sub connection was closed")
			s.candidate.Resign()
			return
		}
	}
}

func main() {
	// Create the ZK connection
	zkConn, _, err := zk.Connect([]string{os.Getenv("ZOOKEEPER_URI")}, time.Second*TIMEOUT)
	if err != nil {
		log.Fatalf("Error in zk.Connect : %v", err)
	}

	defer zkConn.Close()

	// Wait for connection to get a session with the zookeeper
	for zkConn.State().String() != "StateHasSession" {
		time.Sleep(time.Millisecond * 100)
		log.Printf("Waiting for state to get session")
	}

	err = ensureElectionResource(zkConn)

	if err != nil {
		return
	}

	redisConn, err := redis.Dial("tcp", os.Getenv("REDIS_URI"))

	if err != nil {
		log.Fatalf("Error in redis.Dial : %v", err)
	}

	defer redisConn.Close()

	candidate, err := leaderelection.NewElection(zkConn, ELECTION_TOPIC, "")

	if err != nil {
		log.Fatalf("Error creating election candidate")
	}

	s := server{
		redisPubSubConn: &redis.PubSubConn{Conn: redisConn},
		candidate:       candidate,
		pubSubClosed:    make(chan bool),
	}

	go s.processRedisPubSub()

	s.monitorLeadership()

	log.Printf("End of Program")
}
