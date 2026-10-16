// Package generator creates dummy OLTP data for a restaurant:
// reference data (customers, employees, tables, menu items) and
// transactional data (orders, order items, payments).
package generator

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MenuItem is a row from menu_items that the generator keeps in memory.
type MenuItem struct {
	ID    int64
	Name  string
	Price float64
}

// Generator produces dummy data against a Postgres pool.
type Generator struct {
	pool *pgxpool.Pool
	rng  *rand.Rand

	menuItems   []MenuItem
	customerIDs []int64
	waiterIDs   []int64
	tableIDs    []int64
}

// New returns a Generator bound to the pool.
func New(pool *pgxpool.Pool) *Generator {
	return &Generator{
		pool: pool,
		rng:  rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}
}

// ---------------------------------------------------------------------------
// Reference data
// ---------------------------------------------------------------------------

var firstNames = []string{
	"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
	"David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
	"Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
	"Aiden", "Sophia", "Lucas", "Mia", "Ethan", "Amelia", "Mason", "Harper",
	"Logan", "Ella", "Noah", "Aria", "Oliver", "Luna", "Leo", "Chloe",
}

var lastNames = []string{
	"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
	"Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
	"Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
	"Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
	"Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
}

var employeeRoles = []string{"waiter", "waiter", "waiter", "waiter", "waiter", "waiter", "chef", "chef", "chef", "manager", "cashier"}

type menuSeed struct {
	name     string
	category string
	price    float64
}

var menuSeeds = []menuSeed{
	// Appetizers
	{"Garlic Bread", "appetizer", 4.50}, {"Bruschetta", "appetizer", 6.00},
	{"Mozzarella Sticks", "appetizer", 6.50}, {"Buffalo Wings", "appetizer", 8.50},
	{"Calamari", "appetizer", 9.00}, {"Spring Rolls", "appetizer", 5.50},
	{"Spinach Artichoke Dip", "appetizer", 7.50}, {"Chicken Satay", "appetizer", 7.00},
	{"Soup of the Day", "appetizer", 5.00}, {"Onion Rings", "appetizer", 4.50},
	{"Caprese Salad", "appetizer", 7.50}, {"Dumplings (6 pcs)", "appetizer", 6.50},
	// Mains
	{"Margherita Pizza", "main", 12.00}, {"Pepperoni Pizza", "main", 13.50},
	{"Spaghetti Carbonara", "main", 14.00}, {"Lasagna Bolognese", "main", 15.50},
	{"Grilled Salmon", "main", 18.50}, {"Ribeye Steak", "main", 26.00},
	{"Chicken Alfredo", "main", 14.50}, {"Beef Burger", "main", 11.50},
	{"BBQ Pork Ribs", "main", 19.50}, {"Fish and Chips", "main", 13.00},
	{"Chicken Tikka Masala", "main", 15.00}, {"Pad Thai", "main", 12.50},
	{"Nasi Goreng", "main", 11.00}, {"Beef Rendang", "main", 16.00},
	{"Roast Duck", "main", 22.00}, {"Sushi Platter", "main", 24.00},
	{"Ramen Tonkotsu", "main", 13.50}, {"Greek Salad w/ Chicken", "main", 12.00},
	{"Vegan Buddha Bowl", "main", 11.50}, {"Lamb Chops", "main", 25.00},
	// Desserts
	{"Tiramisu", "dessert", 6.50}, {"Chocolate Lava Cake", "dessert", 7.00},
	{"Cheesecake", "dessert", 6.00}, {"Apple Pie", "dessert", 5.50},
	{"Ice Cream Sundae", "dessert", 4.50}, {"Creme Brulee", "dessert", 6.50},
	{"Brownie w/ Ice Cream", "dessert", 6.00}, {"Panna Cotta", "dessert", 5.50},
	// Beverages
	{"Espresso", "beverage", 3.00}, {"Cappuccino", "beverage", 4.00},
	{"Iced Latte", "beverage", 4.50}, {"Fresh Orange Juice", "beverage", 4.50},
	{"Lemonade", "beverage", 3.50}, {"Craft Beer", "beverage", 6.50},
	{"House Red Wine", "beverage", 7.50}, {"House White Wine", "beverage", 7.50},
	{"Sparkling Water", "beverage", 2.50}, {"Iced Tea", "beverage", 3.00},
	{"Mango Smoothie", "beverage", 5.50}, {"Cola", "beverage", 2.50},
}

func pick[T any](rng *rand.Rand, s []T) T {
	return s[rng.IntN(len(s))]
}

// randTimeWeighted returns a time offset (seconds ago) whose hour-of-day is
// biased toward lunch (11:00-14:00) and dinner (18:00-21:00) rushes.
func (g *Generator) randTimeWeighted(now time.Time) time.Time {
	hour := pick(g.rng, []int{
		9, 10, // quiet morning
		11, 12, 12, 13, 13, 14, // lunch rush
		15, 16, 17, // afternoon lull
		18, 19, 19, 20, 20, 21, // dinner rush
		22, // late night
	})
	minute := g.rng.IntN(60)
	dayOffset := g.rng.IntN(90) // spread over the last 90 days
	t := now.AddDate(0, 0, -dayOffset)
	return time.Date(t.Year(), t.Month(), t.Day(), hour, minute, g.rng.IntN(60), 0, t.Location())
}

// SeedReference creates reference data if the database is empty.
// It is safe to call repeatedly.
func (g *Generator) SeedReference(ctx context.Context) error {
	var existing int64
	if err := g.pool.QueryRow(ctx, "SELECT count(*) FROM customers").Scan(&existing); err != nil {
		return fmt.Errorf("check existing data: %w", err)
	}
	if existing > 0 {
		log.Printf("reference data already present (%d customers), skipping seed", existing)
		return g.loadReference(ctx)
	}

	log.Println("seeding reference data...")

	// Customers: 500.
	custRows := make([][]any, 0, 500)
	for id := 1; id <= 500; id++ {
		first, last := pick(g.rng, firstNames), pick(g.rng, lastNames)
		custRows = append(custRows, []any{
			id, first, last,
			fmt.Sprintf("+1-555-%03d-%04d", g.rng.IntN(1000), 1000+id),
			fmt.Sprintf("%s.%s.%d@example.com", lower(first), lower(last), id),
		})
	}
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"customers"},
		[]string{"id", "first_name", "last_name", "phone", "email"}, copyRows(custRows)); err != nil {
		return fmt.Errorf("seed customers: %w", err)
	}

	// Employees: 40.
	empRows := make([][]any, 0, 40)
	for id := 1; id <= 40; id++ {
		hire := time.Now().AddDate(-g.rng.IntN(6), -g.rng.IntN(12), -g.rng.IntN(28))
		empRows = append(empRows, []any{
			id, pick(g.rng, firstNames), pick(g.rng, lastNames),
			pick(g.rng, employeeRoles), hire.Format("2006-01-02"),
		})
	}
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"employees"},
		[]string{"id", "first_name", "last_name", "role", "hire_date"}, copyRows(empRows)); err != nil {
		return fmt.Errorf("seed employees: %w", err)
	}

	// Tables: 30.
	tblRows := make([][]any, 0, 30)
	for id := 1; id <= 30; id++ {
		tblRows = append(tblRows, []any{id, id, 2 + g.rng.IntN(9)}) // capacity 2-10
	}
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"restaurant_tables"},
		[]string{"id", "table_number", "capacity"}, copyRows(tblRows)); err != nil {
		return fmt.Errorf("seed tables: %w", err)
	}

	// Menu items.
	itemRows := make([][]any, 0, len(menuSeeds))
	for id, m := range menuSeeds {
		itemRows = append(itemRows, []any{id + 1, m.name, m.category, m.price})
	}
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"menu_items"},
		[]string{"id", "name", "category", "price"}, copyRows(itemRows)); err != nil {
		return fmt.Errorf("seed menu items: %w", err)
	}

	// Explicit IDs were used, so bring the sequences back in sync.
	for _, table := range []string{"customers", "employees", "restaurant_tables", "menu_items"} {
		if _, err := g.pool.Exec(ctx, fmt.Sprintf(
			`SELECT setval(pg_get_serial_sequence('%s', 'id'), (SELECT MAX(id) FROM %s))`, table, table)); err != nil {
			return fmt.Errorf("sync sequence for %s: %w", table, err)
		}
	}

	return g.loadReference(ctx)
}

// loadReference caches IDs (and menu prices) in memory for fast generation.
func (g *Generator) loadReference(ctx context.Context) error {
	rows, err := g.pool.Query(ctx, "SELECT id, name, price FROM menu_items ORDER BY id")
	if err != nil {
		return err
	}
	g.menuItems = nil
	for rows.Next() {
		var m MenuItem
		var price float64
		if err := rows.Scan(&m.ID, &m.Name, &price); err != nil {
			return err
		}
		m.Price = price
		g.menuItems = append(g.menuItems, m)
	}
	rows.Close()

	if err := g.pool.QueryRow(ctx,
		`SELECT array_agg(id) FROM (SELECT id FROM customers ORDER BY random() LIMIT 500) t`).
		Scan(&g.customerIDs); err != nil {
		return err
	}
	if err := g.pool.QueryRow(ctx,
		`SELECT array_agg(id) FROM employees WHERE role = 'waiter'`).Scan(&g.waiterIDs); err != nil {
		return err
	}
	if err := g.pool.QueryRow(ctx,
		`SELECT array_agg(id ORDER BY id) FROM restaurant_tables`).Scan(&g.tableIDs); err != nil {
		return err
	}

	if len(g.menuItems) == 0 || len(g.customerIDs) == 0 || len(g.waiterIDs) == 0 || len(g.tableIDs) == 0 {
		return fmt.Errorf("reference data is incomplete; run with -init first")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Order generation
// ---------------------------------------------------------------------------

// Backfill inserts n historical orders spread over the last 90 days.
func (g *Generator) Backfill(ctx context.Context, n int) error {
	var maxID, maxItemID, maxPayID int64
	if err := g.pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM orders`).Scan(&maxID); err != nil {
		return err
	}
	if err := g.pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM order_items`).Scan(&maxItemID); err != nil {
		return err
	}
	if err := g.pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) FROM payments`).Scan(&maxPayID); err != nil {
		return err
	}

	const batchSize = 1000
	now := time.Now()
	start := time.Now()
	totalItems, totalPayments := 0, 0

	for done := 0; done < n; done += batchSize {
		size := min(batchSize, n-done)

		orderRows := make([][]any, 0, size)
		itemRows := make([][]any, 0, size*3)
		payRows := make([][]any, 0, size)

		for i := 0; i < size; i++ {
			orderID := maxID + int64(i) + 1
			orderTime := g.randTimeWeighted(now)
			orderRows = append(orderRows, []any{
				orderID,
				pick(g.rng, g.customerIDs),
				pick(g.rng, g.tableIDs),
				pick(g.rng, g.waiterIDs),
				"paid", // finalized below
				orderTime,
			})

			// Line items and running total.
			var amount float64
			for _, item := range g.randomLineItems() {
				maxItemID++
				itemRows = append(itemRows, []any{
					maxItemID, orderID, item.ID, item.Qty, item.UnitPrice,
				})
				amount += float64(item.Qty) * item.UnitPrice
			}

			// Most historical orders are paid; a few were cancelled or never closed out.
			switch roll := g.rng.Float64(); {
			case roll < 0.90: // paid
				orderRows[len(orderRows)-1][4] = "paid"
				maxPayID++
				payRows = append(payRows, []any{
					maxPayID, orderID, pick(g.rng, paymentMethods),
					round2(amount), orderTime.Add(time.Duration(20+g.rng.IntN(50)) * time.Minute),
				})
			case roll < 0.95: // cancelled, no payment
				orderRows[len(orderRows)-1][4] = "cancelled"
			default: // left dangling as pending/served
				orderRows[len(orderRows)-1][4] = pick(g.rng, []string{"pending", "served"})
			}
		}
		maxID += int64(size)
		totalItems += len(itemRows)
		totalPayments += len(payRows)

		if err := g.copyAll(ctx, orderRows, itemRows, payRows); err != nil {
			return err
		}
		log.Printf("backfill: %d/%d orders", min(done+size, n), n)
	}

	// Sync the orders sequence (explicit IDs were used for payments too).
	for _, table := range []string{"orders", "order_items", "payments"} {
		if _, err := g.pool.Exec(ctx, fmt.Sprintf(
			`SELECT setval(pg_get_serial_sequence('%s', 'id'), GREATEST((SELECT MAX(id) FROM %s), 1))`, table, table)); err != nil {
			return fmt.Errorf("sync sequence for %s: %w", table, err)
		}
	}

	log.Printf("backfill complete: %d orders, %d order items, %d payments in %s",
		n, totalItems, totalPayments, time.Since(start).Round(time.Millisecond))
	return nil
}

func (g *Generator) copyAll(ctx context.Context, orders, items, pays [][]any) error {
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"orders"},
		[]string{"id", "customer_id", "table_id", "employee_id", "status", "order_time"}, copyRows(orders)); err != nil {
		return fmt.Errorf("copy orders: %w", err)
	}
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"order_items"},
		[]string{"id", "order_id", "menu_item_id", "quantity", "unit_price"}, copyRows(items)); err != nil {
		return fmt.Errorf("copy order items: %w", err)
	}
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"payments"},
		[]string{"id", "order_id", "method", "amount", "paid_at"}, copyRows(pays)); err != nil {
		return fmt.Errorf("copy payments: %w", err)
	}
	return nil
}

// orderLine is one generated line item.
type orderLine struct {
	ID        int64
	Qty       int
	UnitPrice float64
}

func (g *Generator) randomLineItems() []orderLine {
	lines := make([]orderLine, 0, 5)
	n := 1 + g.rng.IntN(5) // 1-5 distinct items
	seen := make(map[int64]bool, n)
	for len(seen) < n {
		m := pick(g.rng, g.menuItems)
		if seen[m.ID] {
			continue
		}
		seen[m.ID] = true
		lines = append(lines, orderLine{ID: m.ID, Qty: 1 + g.rng.IntN(3), UnitPrice: m.Price})
	}
	return lines
}

var paymentMethods = []string{"cash", "credit_card", "debit_card", "qris", "e_wallet"}

func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }

// ---------------------------------------------------------------------------
// Live streaming
// ---------------------------------------------------------------------------

// Stream continuously inserts new orders like a live POS feed until the
// context is cancelled. Each order progresses pending -> served -> paid
// in real time, with a payment row inserted when it is paid.
func (g *Generator) Stream(ctx context.Context, rate float64) error {
	if rate <= 0 {
		rate = 5
	}
	// Load reference data into memory (seed already ran via -init).
	if err := g.loadReference(ctx); err != nil {
		return fmt.Errorf("load reference data: %w", err)
	}
	interval := time.Duration(float64(time.Second) / rate)

	var (
		wg        sync.WaitGroup
		generated atomic.Int64
		active    atomic.Int64
	)

	// Progress reporter.
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Printf("streaming: %d orders generated, %d in flight", generated.Load(), active.Load())
			}
		}
	}()

	for {
		// Sleep until the next order is due (or shutdown).
		select {
		case <-ctx.Done():
			log.Println("shutting down, waiting for in-flight orders to finish...")
			wg.Wait()
			<-reportDone
			log.Printf("stream stopped. total orders generated: %d", generated.Load())
			return nil
		case <-time.After(jitter(g.rng, interval)):
		}

		generated.Add(1)
		active.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer active.Add(-1)
			if err := g.liveOrder(ctx); err != nil && ctx.Err() == nil {
				log.Printf("order failed: %v", err)
			}
		}()
	}
}

// liveOrder simulates one table's visit: order -> served -> paid.
func (g *Generator) liveOrder(ctx context.Context) error {
	lines := g.randomLineItems()

	var amount float64
	for _, l := range lines {
		amount += float64(l.Qty) * l.UnitPrice
	}

	var orderID int64
	err := g.pool.QueryRow(ctx, `
		INSERT INTO orders (customer_id, table_id, employee_id, status, order_time)
		VALUES ($1, $2, $3, 'pending', now())
		RETURNING id`,
		pick(g.rng, g.customerIDs), pick(g.rng, g.tableIDs), pick(g.rng, g.waiterIDs),
	).Scan(&orderID)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	itemRows := make([][]any, 0, len(lines))
	for _, l := range lines {
		itemRows = append(itemRows, []any{orderID, l.ID, l.Qty, l.UnitPrice})
	}
	if _, err := g.pool.CopyFrom(ctx, pgx.Identifier{"order_items"},
		[]string{"order_id", "menu_item_id", "quantity", "unit_price"}, copyRows(itemRows)); err != nil {
		return fmt.Errorf("insert order items: %w", err)
	}

	// Kitchen prepares the food...
	if err := sleepCtx(ctx, jitter(g.rng, 25*time.Second)); err != nil {
		return err
	}
	if _, err := g.pool.Exec(ctx, `UPDATE orders SET status = 'served' WHERE id = $1`, orderID); err != nil {
		return fmt.Errorf("mark served: %w", err)
	}

	// Customers eat, ask for the bill, pay...
	if err := sleepCtx(ctx, jitter(g.rng, 40*time.Second)); err != nil {
		return err
	}
	if _, err := g.pool.Exec(ctx, `UPDATE orders SET status = 'paid' WHERE id = $1`, orderID); err != nil {
		return fmt.Errorf("mark paid: %w", err)
	}
	if _, err := g.pool.Exec(ctx, `
		INSERT INTO payments (order_id, method, amount, paid_at)
		VALUES ($1, $2, $3, now())`,
		orderID, pick(g.rng, paymentMethods), round2(amount)); err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// copySource adapts a materialized [][]any slice to pgx.CopyFromSource.
type copySource struct {
	rows [][]any
	i    int
}

func copyRows(rows [][]any) pgx.CopyFromSource { return &copySource{rows: rows} }

func (s *copySource) Next() bool { return s.i < len(s.rows) }

func (s *copySource) Values() ([]any, error) {
	v := s.rows[s.i]
	s.i++
	return v, nil
}

func (s *copySource) Err() error { return nil }

// jitter randomly varies d by +-30% so traffic looks organic.
func jitter(rng *rand.Rand, d time.Duration) time.Duration {
	f := 0.7 + rng.Float64()*0.6
	return time.Duration(float64(d) * f)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}
