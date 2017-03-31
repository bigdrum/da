package da_test

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"gopkg.in/ory-am/dockertest.v3"
)

var sqlDB *sql.DB

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	dockerHost := func() string {
		hostName := "127.0.0.1"
		if host := os.Getenv("DOCKER_HOST"); host != "" {
			u, err := url.Parse(host)
			if err != nil {
				log.Fatalln("invalid DOCKER_HOST:", err)
			}
			hostName, _, err = net.SplitHostPort(u.Host)
			if err != nil {
				log.Fatalln("invalid DOCKER_HOST:", err)
			}
		}
		return hostName
	}()

	code := func() int {
		// pulls an image, creates a container based on it and runs it
		resource, err := pool.Run("postgres", "9.5-alpine", []string{})
		if err != nil {
			log.Fatalf("Could not start resource: %s", err)
		}
		defer func() {
			if err := pool.Purge(resource); err != nil {
				log.Fatalf("Could not purge resource: %s", err)
			}
		}()

		spec := fmt.Sprintf("postgres://postgres@%s:%s/postgres?sslmode=disable", dockerHost, resource.GetPort("5432/tcp"))
		fmt.Printf("Postgres pulled: %s\n", spec)

		// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
		if err := pool.Retry(func() error {
			var err error
			sqlDB, err = sql.Open("postgres", spec)
			if err != nil {
				return err
			}
			err = sqlDB.Ping()
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			log.Fatalf("Could not connect to docker: %s", err)
		}
		fmt.Println("Postgres started.")

		return m.Run()
	}()
	os.Exit(code)
}
