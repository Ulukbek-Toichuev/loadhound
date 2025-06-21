/*
LoadHound — Relentless SQL load testing tool.
Copyright © 2025 Toichuev Ulukbek t.ulukbek01@gmail.com

Licensed under the MIT License.
*/

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Ulukbek-Toichuev/loadhound/cmd"
	"github.com/Ulukbek-Toichuev/loadhound/pkg"
	_ "github.com/lib/pq"
)

// func main() {
// 	const connStr = "postgres://postgres:Rewq_1234@185.121.233.53:5432/online-store?sslmode=disable"
// 	db, err := sql.Open("postgres", connStr)
// 	if err != nil {
// 		panic(err)
// 	}

// 	query := `SELECT id, name, stock FROM products WHERE stock > $1 AND is_deleted = FALSE ORDER BY stock ASC LIMIT $2;`
// 	stm, err := db.Prepare(query)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer stm.Close()

// 	time.Sleep(10 * time.Second)
// 	rows, err := stm.Query(30, 2)
// 	if err != nil {
// 		panic(err)
// 	}

// 	defer rows.Close()

// 	c := 0
// 	for rows.Next() {
// 		c++
// 	}
// 	fmt.Printf("rows count: %d\n", c)
// }

func init() {
	pkg.PrintAsciiArtLogo()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	cmd.Execute(ctx)
}
