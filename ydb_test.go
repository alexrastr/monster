package monster

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/joho/godotenv"
)

func loadEnv(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}
}

func TestOpen_Success(t *testing.T) {
	loadEnv(t)

	ctx := context.Background()
	dsn := os.Getenv("DSN")
	keyPath := os.Getenv("KEY_PATH")

	client := NewYDBClient(
		ctx,
		dsn,
		keyPath,
	)

	err := client.Open()

	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}

	defer func() {
		err := client.Close()
		if err != nil {
			t.Errorf("Expected nil error, got: %v", err)
		}
	}()

}

func TestGetUser(t *testing.T) {
	loadEnv(t)

	ctx := context.Background()
	dsn := os.Getenv("DSN")
	keyPath := os.Getenv("KEY_PATH")

	client := NewYDBClient(
		ctx,
		dsn,
		keyPath,
	)

	err := client.Open()

	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}

	defer func() {
		err := client.Close()
		if err != nil {
			t.Errorf("Expected nil error, got: %v", err)
		}
	}()

	click, err := client.GetUserClick(540969473)

	if err != nil {
		t.Errorf("Expected nil error, got: %v", err)
	}

	t.Logf(string(strconv.Itoa(int(click))))
}
