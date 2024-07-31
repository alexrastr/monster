package monster

import (
	"context"
	"fmt"
	"log"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	yc "github.com/ydb-platform/ydb-go-yc"
)

// YDBClient реализация Database для работы с Yandex Database
type YDBClient struct {
	dsn     string
	keyPath string
	db      *ydb.Driver
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewYDBClient создаёт новый экземпляр YDBClient
func NewYDBClient(ctx context.Context, dsn, keyPath string) *YDBClient {
	ctx, cancel := context.WithCancel(ctx)
	return &YDBClient{
		dsn:     dsn,
		keyPath: keyPath,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Open открывает соединение с YDB
func (client *YDBClient) Open() error {
	var authOption ydb.Option

	if client.keyPath == "" {
		authOption = yc.WithMetadataCredentials()
	} else {
		authOption = yc.WithServiceAccountKeyFileCredentials(client.keyPath)
	}

	fmt.Println("Connecting to YDB...")

	db, err := ydb.Open(client.ctx, client.dsn, authOption, yc.WithInternalCA())
	if err != nil {
		return fmt.Errorf("failed to open YDB connection: %w", err)
	}
	client.db = db
	return nil
}

// Close закрывает соединение с YDB
func (client *YDBClient) Close() error {
	if client.db == nil {
		return fmt.Errorf("database connection is not open")
	}
	client.cancel() // Отмена контекста для завершения всех операций
	return client.db.Close(client.ctx)
}

// GetUserClick получает количество кликов пользователя по его telegram_id
func (client *YDBClient) GetUserClick(telegramID uint64) (uint64, error) {
	if client.db == nil {
		return 0, fmt.Errorf("database connection is not open")
	}

	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	var click uint64

	err := client.db.Table().Do(client.ctx, func(ctx context.Context, s table.Session) error {
		var res result.Result

		query := `
			DECLARE $telegram_id AS Uint64;
			SELECT click FROM users WHERE telegram_id = $telegram_id;
		`
		_, res, err := s.Execute(
			ctx,
			readTx,
			query,
			table.NewQueryParameters(
				table.ValueParam("$telegram_id", types.Uint64Value(telegramID)), // Подстановка параметра
			),
		)
		if err != nil {
			return fmt.Errorf("query execution failed: %w", err)
		}
		defer res.Close()

		log.Printf("> Executing query: GetUserClick\n")
		for res.NextResultSet(ctx) {
			for res.NextRow() {
				err = res.ScanNamed(
					named.Required("click", &click),
				)
				if err != nil {
					return fmt.Errorf("failed to scan result: %w", err)
				}
				log.Printf("  > Clicks: %d\n", click)
			}
		}
		return res.Err()
	})
	if err != nil {
		return 0, err
	}

	return click, nil
}
