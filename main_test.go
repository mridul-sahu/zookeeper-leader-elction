package main

import (
	"testing"
	"github.com/ory/dockertest"
	"log"
	"os"
	"time"
	"github.com/samuel/go-zookeeper/zk"
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %v", err)
	}

	// pulls an image, creates a container based on it and runs it
	dbResource, err := pool.Run("mongo", "latest", nil)
	if err != nil {
		log.Fatalf("Could not start DB Resource: %v", err)
	}

	rdResource, err := pool.Run("redis", "latest", nil)
	if err != nil {
		log.Fatalf("Could not start Redis Resource: %v", err)
	}

	zkResource, err := pool.Run("zookeeper", "latest", nil)

	if err != nil {
		log.Fatalf("Could not start Zookeeper resource: %v", err)
	}

	os.Setenv("DB_URI", "localhost:"+dbResource.GetPort("27017/tcp"))
	os.Setenv("REDIS_URI", "localhost:"+rdResource.GetPort("6379/tcp"))
	os.Setenv("ZOOKEEPER_URI", "localhost:"+zkResource.GetPort("2181/tcp"))


	code := m.Run()

	if err := pool.Purge(dbResource); err != nil {
		log.Fatalf("Could not purge resource: %v", err)
	}

	if err := pool.Purge(rdResource); err != nil {
		log.Fatalf("Could not purge resource: %v", err)
	}

	if err := pool.Purge(zkResource); err != nil {
		log.Fatalf("Could not purge resource: %v", err)
	}

	os.Exit(code)
}

func TestEnsureElectionResource(t *testing.T) {
	zkConn, _, err := zk.Connect([]string{os.Getenv("ZOOKEEPER_URI")}, time.Second*TIMEOUT)
	if err != nil {
		log.Printf("Error in zk.Connect: %v", err)
		t.FailNow()
	}

	defer zkConn.Close()

	for zkConn.State().String() != "StateHasSession" {
		time.Sleep(time.Millisecond * 100)
		log.Printf("Waiting for state to get session")
	}

	err = ensureElectionResource(zkConn)

	if err != nil {
		log.Printf("Ensure Election Resource failed")
		t.FailNow()
	}

	ok, _, err := zkConn.Exists(ELECTION_TOPIC)

	if err != nil {
		log.Printf("Check exists failed: %v", err)
		t.FailNow()
	}

	if !ok {
		log.Printf("Election Topic (%s) does not exists", ELECTION_TOPIC)
		t.FailNow()
	}

	err = ensureElectionResource(zkConn)

	if err != nil {
		log.Printf("Ensure Election Resource failed")
		t.FailNow()
	}
}