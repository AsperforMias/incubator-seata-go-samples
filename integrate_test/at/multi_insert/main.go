/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"seata.apache.org/seata-go/pkg/client"
	sql2 "seata.apache.org/seata-go/pkg/datasource/sql"
	"seata.apache.org/seata-go/pkg/tm"
)

const testTable = "multi_insert_order_tbl"

var db *sql.DB

func main() {
	initConfig()
	defer db.Close()

	ctx := context.Background()
	if err := cleanupData(ctx); err != nil {
		log.Fatalf("cleanup data failed: %v", err)
	}

	err := tm.WithGlobalTx(context.Background(), &tm.GtxConfig{
		Name:    "ATSampleLocalGlobalTx_MultiInsert",
		Timeout: time.Second * 30,
	}, insertData)
	if err != nil {
		log.Fatalf("failed to execute transaction: %v", err)
	}

	if err := checkData(ctx); err != nil {
		log.Fatalf("check data failed: %v", err)
	}

	time.Sleep(time.Second * 10)
	if err := checkUndoLogData(ctx); err != nil {
		log.Fatalf("check undo log failed: %v", err)
	}

	fmt.Println("multi_insert test passed")
}

func initConfig() {
	client.InitPath("./conf/seatago.yml")
	initDB()
}

func initDB() {
	var err error
	db, err = sql.Open(sql2.SeataATMySQLDriver, "root:12345678@tcp(127.0.0.1:3306)/seata_client?multiStatements=true&interpolateParams=true")
	if err != nil {
		panic("init service error")
	}
}

func cleanupData(ctx context.Context) error {
	if _, err := db.ExecContext(ctx, "DELETE FROM "+testTable); err != nil {
		return err
	}
	return nil
}

func insertData(ctx context.Context) error {
	_, err := db.ExecContext(ctx,
		"INSERT INTO "+testTable+" (tenant_id, user_id, commodity_code, count, money, descs) VALUES "+
			"(?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
		int64(1001), "NO-100101", "C100101", int64(11), int64(101), "multi insert auto 1",
		int64(1002), "NO-100102", "C100102", int64(12), int64(102), "multi insert auto 2")
	if err != nil {
		return fmt.Errorf("insert mixed composite primary keys failed: %w", err)
	}

	_, err = db.ExecContext(ctx,
		"INSERT INTO "+testTable+" VALUES "+
			"(?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?)",
		int64(2001), int64(2001), "NO-200101", "C200101", int64(21), int64(201), "multi insert explicit 1",
		int64(2002), int64(2002), "NO-200102", "C200102", int64(22), int64(202), "multi insert explicit 2")
	if err != nil {
		return fmt.Errorf("insert explicit composite primary keys failed: %w", err)
	}

	return nil
}

func checkData(ctx context.Context) error {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+testTable+" WHERE "+
			"(tenant_id = ? AND user_id = ? AND commodity_code = ? AND count = ? AND money = ? AND descs = ?) OR "+
			"(tenant_id = ? AND user_id = ? AND commodity_code = ? AND count = ? AND money = ? AND descs = ?) OR "+
			"(id = ? AND tenant_id = ? AND user_id = ? AND commodity_code = ? AND count = ? AND money = ? AND descs = ?) OR "+
			"(id = ? AND tenant_id = ? AND user_id = ? AND commodity_code = ? AND count = ? AND money = ? AND descs = ?)",
		int64(1001), "NO-100101", "C100101", int64(11), int64(101), "multi insert auto 1",
		int64(1002), "NO-100102", "C100102", int64(12), int64(102), "multi insert auto 2",
		int64(2001), int64(2001), "NO-200101", "C200101", int64(21), int64(201), "multi insert explicit 1",
		int64(2002), int64(2002), "NO-200102", "C200102", int64(22), int64(202), "multi insert explicit 2").
		Scan(&count)
	if err != nil {
		return err
	}
	if count != 4 {
		return fmt.Errorf("expected 4 inserted rows, got %d", count)
	}
	return nil
}

func checkUndoLogData(ctx context.Context) error {
	var count int64
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM undo_log").Scan(&count)
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("check undolog failed")
	}
	return nil
}
