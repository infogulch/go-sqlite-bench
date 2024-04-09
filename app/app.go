package app

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

func Run(makeDb func(dbfile string) Db) {
	log.SetOutput(os.Stdout)
	log.SetFlags(0)
	log.Print("")

	benchmarks := map[string]*bool{
		"simple":     new(bool),
		"complex":    new(bool),
		"many":       new(bool),
		"large":      new(bool),
		"concurrent": new(bool),
		"wal":        new(bool),
		"bulk":       new(bool),
	}
	for name, p := range benchmarks {
		flag.BoolVar(p, name, false, "")
	}
	flag.Parse()
	dbfile := flag.Arg(0)
	if dbfile == "" {
		log.Fatal("dbfile empty, cannot bench")
	}

	// verbose
	const verbose = false
	if verbose {
		log.Printf("dbfile %q", dbfile)
	}
	if *benchmarks["simple"] {
		benchSimple(dbfile, verbose, makeDb)
	}
	if *benchmarks["bulk"] {
		benchSimpleBulk(dbfile, verbose, makeDb)
	}
	if *benchmarks["complex"] {
		benchComplex(dbfile, verbose, makeDb)
	}
	if *benchmarks["many"] {
		benchMany(dbfile, verbose, 10, makeDb)
		benchMany(dbfile, verbose, 100, makeDb)
		benchMany(dbfile, verbose, 1_000, makeDb)
	}
	if *benchmarks["large"] {
		benchLarge(dbfile, verbose, 50_000, makeDb)
		benchLarge(dbfile, verbose, 100_000, makeDb)
		benchLarge(dbfile, verbose, 200_000, makeDb)
	}
	if *benchmarks["concurrent"] {
		benchConcurrent(dbfile, verbose, 2, makeDb)
		benchConcurrent(dbfile, verbose, 4, makeDb)
		benchConcurrent(dbfile, verbose, 8, makeDb)
	}
	if *benchmarks["wal"] {
		benchWal(dbfile, verbose, 1, makeDb)
		benchWal(dbfile, verbose, 2, makeDb)
		benchWal(dbfile, verbose, 4, makeDb)
		benchWal(dbfile, verbose, 8, makeDb)
		benchWal(dbfile, verbose, 16, makeDb)
	}
}

const insertUserSql = "INSERT INTO users(id,created,email,active) VALUES(?,?,?,?)"
const insertArticleSql = "INSERT INTO articles(id,created,userId,text) VALUES(?,?,?,?)"
const insertCommentSql = "INSERT INTO comments(id,created,articleId,text) VALUES(?,?,?,?)"

func initJournalDelete(db Db) {
	db.Exec(
		"PRAGMA journal_mode=DELETE",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=1",
		"PRAGMA busy_timeout=5000", // 5s busy timeout
	)
	initSchema(db)
}

func initJournalWal(db Db) {
	db.Exec(
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=normal",
		"PRAGMA foreign_keys=1",
		"PRAGMA busy_timeout=20000", // 20s busy timeout
	)
	initSchema(db)
}

func initSchema(db Db) {
	db.Exec(
		"CREATE TABLE users ("+
			"id INTEGER PRIMARY KEY NOT NULL,"+
			" created INTEGER NOT NULL,"+ // time.Time
			" email TEXT NOT NULL,"+
			" active INTEGER NOT NULL)", // bool
		"CREATE INDEX users_created ON users(created)",
		"CREATE TABLE articles ("+
			"id INTEGER PRIMARY KEY NOT NULL,"+
			" created INTEGER NOT NULL, "+ // time.Time
			" userId INTEGER NOT NULL REFERENCES users(id),"+
			" text TEXT NOT NULL)",
		"CREATE INDEX articles_created ON articles(created)",
		"CREATE INDEX articles_userId ON articles(userId)",
		"CREATE TABLE comments ("+
			"id INTEGER PRIMARY KEY NOT NULL,"+
			" created INTEGER NOT NULL, "+ // time.Time
			" articleId INTEGER NOT NULL REFERENCES articles(id),"+
			" text TEXT NOT NULL)",
		"CREATE INDEX comments_created ON comments(created)",
		"CREATE INDEX comments_articleId ON comments(articleId)",
	)
}

// Insert 1 million user rows in one database transaction.
// Then query all users once.
func benchSimple(dbfile string, verbose bool, makeDb func(dbfile string) Db) {
	removeDbfiles(dbfile)
	db := makeDb(dbfile)
	defer db.Close()
	initJournalDelete(db)
	// insert users
	var users []User
	base := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local)
	const nusers = 1_000_000
	for i := 0; i < nusers; i++ {
		users = append(users, NewUser(
			i+1,                                      // id,
			base.Add(time.Duration(i)*time.Minute),   // created,
			fmt.Sprintf("user%08d@example.com", i+1), // email,
			true,                                     // active,
		))
	}
	t0 := time.Now()
	db.InsertUsers("INSERT INTO users(id,created,email,active) VALUES(?,?,?,?)", users)
	insertMillis := millisSince(t0)
	if verbose {
		log.Printf("  insert took %d ms", insertMillis)
	}
	// query users
	t0 = time.Now()
	users = db.FindUsers("SELECT id,created,email,active FROM users ORDER BY id")
	MustBeEqual(len(users), nusers)
	queryMillis := millisSince(t0)
	if verbose {
		log.Printf("  query took %d ms", queryMillis)
	}
	// validate query result
	for i, u := range users {
		MustBeEqual(i+1, u.Id)
		Must(2023 <= u.Created.Year() && u.Created.Year() <= 2025, "wrong created year in %v", u.Created)
		MustBeEqual("user0", u.Email[0:5])
		MustBeEqual(true, u.Active)
	}
	// print results
	bench := "1_simple"
	log.Printf("%s - insert - %-10s - %10d", bench, db.DriverName(), insertMillis)
	log.Printf("%s - query  - %-10s - %10d", bench, db.DriverName(), queryMillis)
	log.Printf("%s - dbsize - %-10s - %10d", bench, db.DriverName(), dbsize(dbfile))
}

// Insert 200 users in one database transaction.
// Then insert 20000 articles (100 articles for each user) in another transaction.
// Then insert 400000 articles (20 comments for each article) in another transaction.
// Then query all users, articles and comments in one big JOIN statement.
func benchComplex(dbfile string, verbose bool, makeDb func(dbfile string) Db) {
	removeDbfiles(dbfile)
	db := makeDb(dbfile)
	defer db.Close()
	initJournalDelete(db)
	const nusers = 200
	const narticlesPerUser = 100
	const ncommentsPerArticle = 20
	if verbose {
		log.Printf("nusers = %d", nusers)
		log.Printf("narticlesPerUser = %d", narticlesPerUser)
		log.Printf("ncommentsPerArticle = %d", ncommentsPerArticle)
	}
	// make users, articles, comments
	var users []User
	var articles []Article
	var comments []Comment
	base := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local)
	var userId int
	var articleId int
	var commentId int
	for u := 0; u < nusers; u++ {
		userId++
		user := NewUser(
			userId,                                   // Id
			base.Add(time.Duration(u)*time.Minute),   // Created
			fmt.Sprintf("user%08d@example.com", u+1), // Email
			u%2 == 0,                                 // Active
		)
		users = append(users, user)
		for a := 0; a < narticlesPerUser; a++ {
			articleId++
			article := NewArticle(
				articleId, // Id
				base.Add(time.Duration(u)*time.Minute).Add(time.Duration(a)*time.Second), // Created
				userId,         // UserId
				"article text", // Text
			)
			articles = append(articles, article)
			for c := 0; c < ncommentsPerArticle; c++ {
				commentId++
				comment := NewComment(
					commentId,
					base.Add(time.Duration(u)*time.Minute).Add(time.Duration(a)*time.Second).Add(time.Duration(c)*time.Millisecond), // created,
					articleId,
					"comment text", // text,
				)
				comments = append(comments, comment)
			}
		}
	}
	// insert users, articles, comments
	t0 := time.Now()
	db.InsertUsers(insertUserSql, users)
	db.InsertArticles(insertArticleSql, articles)
	db.InsertComments(insertCommentSql, comments)
	insertMillis := millisSince(t0)
	if verbose {
		log.Printf("  insert took %d ms", insertMillis)
	}
	// query users, articles, comments in one big join
	querySql := "SELECT" +
		" users.id, users.created, users.email, users.active," +
		" articles.id, articles.created, articles.userId, articles.text," +
		" comments.id, comments.created, comments.articleId, comments.text" +
		" FROM users" +
		" LEFT JOIN articles ON articles.userId = users.id" +
		" LEFT JOIN comments ON comments.articleId = articles.id" +
		" ORDER BY users.created,  articles.created, comments.created"
	t0 = time.Now()
	users, articles, comments = db.FindUsersArticlesComments(querySql)
	queryMillis := millisSince(t0)
	if verbose {
		log.Printf("  query took %d ms", queryMillis)
	}
	// validate query result
	MustBeEqual(nusers, len(users))
	MustBeEqual(nusers*narticlesPerUser, len(articles))
	MustBeEqual(nusers*narticlesPerUser*ncommentsPerArticle, len(comments))
	for i, user := range users {
		MustBeEqual(i+1, user.Id)
		MustBeEqual(2023, user.Created.Year())
		MustBeEqual("user0", user.Email[0:5])
		MustBeEqual(i%2 == 0, user.Active)
	}
	for i, article := range articles {
		MustBeEqual(i+1, article.Id)
		MustBeEqual(2023, article.Created.Year())
		MustBe(article.UserId >= 1)
		MustBe(article.UserId <= 1+nusers)
		MustBeEqual("article text", article.Text)
		if i > 0 {
			last := articles[i-1]
			MustBe(article.UserId >= last.UserId)
		}
	}
	for i, comment := range comments {
		MustBeEqual(i+1, comment.Id)
		MustBeEqual(2023, comment.Created.Year())
		MustBe(comment.ArticleId >= 1)
		MustBe(comment.ArticleId <= 1+(nusers*narticlesPerUser))
		MustBeEqual("comment text", comment.Text)
		if i > 0 {
			last := comments[i-1]
			MustBe(comment.ArticleId >= last.ArticleId)
		}
	}
	// print results
	bench := "2_complex"
	log.Printf("%s - insert - %-10s - %10d", bench, db.DriverName(), insertMillis)
	log.Printf("%s - query  - %-10s - %10d", bench, db.DriverName(), queryMillis)
	log.Printf("%s - dbsize - %-10s - %10d", bench, db.DriverName(), dbsize(dbfile))
}

// Insert N users in one database transaction.
// Then query all users 1000 times.
// This benchmark is used to simluate a read-heavy use case.
func benchMany(dbfile string, verbose bool, nusers int, makeDb func(dbfile string) Db) {
	removeDbfiles(dbfile)
	db := makeDb(dbfile)
	defer db.Close()
	initJournalDelete(db)
	// insert users
	var users []User
	base := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local)
	for i := 0; i < nusers; i++ {
		users = append(users, NewUser(
			i+1,                                      // id,
			base.Add(time.Duration(i)*time.Minute),   // created,
			fmt.Sprintf("user%08d@example.com", i+1), // email,
			true,                                     // active,
		))
	}
	t0 := time.Now()
	db.InsertUsers(insertUserSql, users)
	insertMillis := millisSince(t0)
	if verbose {
		log.Printf("  insert took %d ms", insertMillis)
	}
	// query users 1000 times
	t0 = time.Now()
	for i := 0; i < 1000; i++ {
		users = db.FindUsers("SELECT id,created,email,active FROM users ORDER BY id")
		MustBeEqual(len(users), nusers)
	}
	queryMillis := millisSince(t0)
	if verbose {
		log.Printf("  query took %d ms", queryMillis)
	}
	// validate query result
	for i, u := range users {
		MustBeEqual(i+1, u.Id)
		MustBeEqual(2023, u.Created.Year())
		MustBeEqual("user0", u.Email[0:5])
		MustBeEqual(true, u.Active)
	}
	// print results
	bench := fmt.Sprintf("3_many/%04d", nusers)
	log.Printf("%s - insert - %-10s - %10d", bench, db.DriverName(), insertMillis)
	log.Printf("%s - query  - %-10s - %10d", bench, db.DriverName(), queryMillis)
	log.Printf("%s - dbsize - %-10s - %10d", bench, db.DriverName(), dbsize(dbfile))
}

// Insert 10000 users with N bytes of row content.
// Then query all users.
// This benchmark is used to simluate reading of large (gigabytes) databases.
func benchLarge(dbfile string, verbose bool, nsize int, makeDb func(dbfile string) Db) {
	removeDbfiles(dbfile)
	db := makeDb(dbfile)
	defer db.Close()
	initJournalDelete(db)
	// insert user with large emails
	t0 := time.Now()
	base := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local)
	const nusers = 10_000
	var users []User
	for i := 0; i < nusers; i++ {
		users = append(users, NewUser(
			i+1,                                    // Id
			base.Add(time.Duration(i)*time.Second), // Created
			strings.Repeat("a", nsize),             // Email
			true,                                   // Active
		))
	}
	db.InsertUsers(insertUserSql, users)
	insertMillis := millisSince(t0)
	// query users
	t0 = time.Now()
	users = db.FindUsers("SELECT id,created,email,active FROM users ORDER BY id")
	MustBeEqual(len(users), nusers)
	queryMillis := millisSince(t0)
	if verbose {
		log.Printf("  query took %d ms", queryMillis)
	}
	// validate query result
	for i, u := range users {
		MustBeEqual(i+1, u.Id)
		MustBeEqual(2023, u.Created.Year())
		MustBeEqual("a", u.Email[0:1])
		MustBeEqual(true, u.Active)
	}
	// print results
	bench := fmt.Sprintf("4_large/%06d", nsize)
	log.Printf("%s - insert - %-10s - %10d", bench, db.DriverName(), insertMillis)
	log.Printf("%s - query  - %-10s - %10d", bench, db.DriverName(), queryMillis)
	log.Printf("%s - dbsize - %-10s - %10d", bench, db.DriverName(), dbsize(dbfile))
}

// Insert one million users.
// Then have N goroutines query all users.
// This benchmark is used to simulate concurrent reads.
func benchConcurrent(dbfile string, verbose bool, ngoroutines int, makeDb func(dbfile string) Db) {
	removeDbfiles(dbfile)
	db1 := makeDb(dbfile)
	driverName := db1.DriverName()
	initJournalDelete(db1)
	// insert many users
	base := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local)
	const nusers = 1_000_000
	var users []User
	for i := 0; i < nusers; i++ {
		users = append(users, NewUser(
			i+1,                                    // Id
			base.Add(time.Duration(i)*time.Second), // Created
			fmt.Sprintf("user%d@example.com", i+1), // Email
			true,                                   // Active
		))
	}
	t0 := time.Now()
	db1.InsertUsers(insertUserSql, users)
	db1.Close()
	insertMillis := millisSince(t0)
	// query users in N goroutines
	t0 = time.Now()
	var wg sync.WaitGroup
	for i := 0; i < ngoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := makeDb(dbfile)
			db.Exec(
				"PRAGMA foreign_keys=1",
				"PRAGMA busy_timeout=5000", // 5s busy timeout
			)
			defer db.Close()
			users = db.FindUsers("SELECT id,created,email,active FROM users ORDER BY id")
			MustBeEqual(len(users), nusers)
			// validate query result
			for i, u := range users {
				MustBeEqual(i+1, u.Id)
				MustBeEqual(2023, u.Created.Year())
				MustBeEqual("user", u.Email[0:4])
				MustBeEqual(true, u.Active)
			}
		}()
	}
	// wait for completion
	wg.Wait()
	queryMillis := millisSince(t0)
	if verbose {
		log.Printf("  query took %d ms", queryMillis)
	}
	// print results
	bench := fmt.Sprintf("5_concurrent/%d", ngoroutines)
	log.Printf("%s - insert - %-10s - %10d", bench, driverName, insertMillis)
	log.Printf("%s - query  - %-10s - %10d", bench, driverName, queryMillis)
	log.Printf("%s - dbsize - %-10s - %10d", bench, driverName, dbsize(dbfile))
}

// Generate 1M users
// Then split it up into N batches to pass to N goroutines where each goroutine:
// Splits its batch into 10 chunks, and for each chunk:
// Inserts the chunk of users, then queries all users
func benchWal(dbfile string, _verbose bool, ngoroutines int, makeDb func(dbfile string) Db) {
	removeDbfiles(dbfile)
	db1 := makeDb(dbfile)
	driverName := db1.DriverName()
	initJournalWal(db1)
	db1.Close()
	// Prepare many users
	base := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local)
	const nusers = 1_000_000
	var users []User
	for i := 0; i < nusers; i++ {
		users = append(users, NewUser(
			i+1,                                    // Id
			base.Add(time.Duration(i)*time.Second), // Created
			fmt.Sprintf("user%d@example.com", i+1), // Email
			true,                                   // Active
		))
	}
	chunkUsers := func(u []User, n int) [][]User {
		cn := len(u) / n
		cu := make([][]User, 0, n)
		for i := range n {
			cu = append(cu, u[i*cn:(i+1)*cn])
		}
		if len(u) > cn*n {
			cu[n-1] = u[(n-1)*cn:] // put any leftover in the last chunk
		}
		return cu
	}
	// run routine to insert then query users in N goroutines
	t0 := time.Now()
	var wg sync.WaitGroup
	for _, chunk := range chunkUsers(users, ngoroutines) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := makeDb(dbfile)
			db.Exec(
				"PRAGMA journal_mode=WAL",
				"PRAGMA synchronous=normal",
				"PRAGMA foreign_keys=1",
				"PRAGMA busy_timeout=20000", // 20s busy timeout
			)
			defer db.Close()
			chunks := chunkUsers(chunk, 10)
			checkChunk := func(i int) {
				chunk := chunks[i]
				first, last := chunk[0].Id, chunk[len(chunk)-1].Id
				users := db.FindUsers(fmt.Sprintf("SELECT id,created,email,active FROM users WHERE id BETWEEN %d AND %d ORDER BY id", first, last))
				MustBeEqual(len(chunk), len(users))
				// validate query result
				for i, u := range users {
					MustBeEqual(i+first, u.Id)
					MustBeEqual(2023, u.Created.Year())
					MustBeEqual("user", u.Email[0:4])
					MustBeEqual(true, u.Active)
				}
			}
			for i, chunk := range chunks {
				// insert chunk of users
				db.InsertUsers(insertUserSql, chunk)
				// read back last three chunks of users
				for j := range 3 {
					if i-j >= 0 {
						checkChunk(i - j)
					}
				}
			}
		}()
	}
	// wait for completion
	wg.Wait()
	queryMillis := millisSince(t0)
	// print results
	bench := fmt.Sprintf("6_wal/%-2d", ngoroutines)
	log.Printf("%s - insert_query - %-10s - %10d", bench, driverName, queryMillis)
	log.Printf("%s - dbsize       - %-10s - %10d", bench, driverName, dbsize(dbfile))
}

// Insert 1 million user rows in one database transaction using bulk insert
// strategy 1.
// Then query all users once.
func benchSimpleBulk(dbfile string, verbose bool, makeDb func(string) Db) {
	removeDbfiles(dbfile)
	db := makeDb(dbfile)
	defer db.Close()
	initJournalDelete(db)
	// insert users
	var users []User
	base := time.Date(2023, 10, 1, 10, 0, 0, 0, time.Local)
	const nusers = 1_000_000
	for i := 0; i < nusers; i++ {
		users = append(users, NewUser(
			i+1,                                      // id,
			base.Add(time.Duration(i)*time.Minute),   // created,
			fmt.Sprintf("user%08d@example.com", i+1), // email,
			true,                                     // active,
		))
	}
	t0 := time.Now()
	db.(BulkDb).InsertUsersBulk("INSERT INTO users(id,created,email,active) VALUES %s", users)
	insertMillis := millisSince(t0)
	if verbose {
		log.Printf("  insert took %d ms", insertMillis)
	}
	// query users
	t0 = time.Now()
	users = db.FindUsers("SELECT id,created,email,active FROM users ORDER BY id")
	MustBeEqual(len(users), nusers)
	queryMillis := millisSince(t0)
	if verbose {
		log.Printf("  query took %d ms", queryMillis)
	}
	// validate query result
	for i, u := range users {
		MustBeEqual(i+1, u.Id)
		Must(2023 <= u.Created.Year() && u.Created.Year() <= 2025, "wrong created year in %v", u.Created)
		MustBeEqual("user0", u.Email[0:5])
		MustBeEqual(true, u.Active)
	}
	// print results
	bench := "7_bulk"
	log.Printf("%s - insert - %-10s - %10d", bench, db.DriverName(), insertMillis)
	log.Printf("%s - query  - %-10s - %10d", bench, db.DriverName(), queryMillis)
	log.Printf("%s - dbsize - %-10s - %10d", bench, db.DriverName(), dbsize(dbfile))
}
