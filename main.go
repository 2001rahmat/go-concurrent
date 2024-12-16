package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	_ "github.com/godror/godror"

)

// Struct untuk product
type Product struct {
	Name  string
	Price float64
}

// Fungsi untuk menghubungkan ke database
func connectDB() (*sql.DB, error) {
	dbConfig := struct {
		Host     string
		Port     string
		Username string
		Password string
		Schema   string
	}{
		Host:     "---",
		Port:     "---",
		Username: "---",
		Password: "---",
		Schema:   "---",
	}

	connectionString := fmt.Sprintf("%s/%s@%s:%s/%s", dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Schema)

	db, err := sql.Open("godror", connectionString)
	if err != nil {
		return nil, fmt.Errorf("gagal membuka koneksi ke database: %w", err)
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("gagal terhubung ke database: %w", err)
	}

	log.Println("Berhasil terhubung ke database Oracle!")
	return db, nil
}

// Fungsi insert data
func insertProduct(db *sql.DB, product Product) error {
	_, err := db.Exec("BEGIN INSERT INTO GO_PRODUCT (NAME, PRICE) VALUES (:1, :2); END;", product.Name, product.Price)
	return err
}

// Fungsi get data
func getProduct(db *sql.DB) ([]Product, error) {
	rows, err := db.Query("SELECT NAME, PRICE FROM GO_PRODUCT")
	if err != nil {
		return nil, fmt.Errorf("gagal menjalankan query: %w", err)
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var product Product
		if err := rows.Scan(&product.Name, &product.Price); err != nil {
			return nil, fmt.Errorf("terjadi error saat iterasi rows: %w", err)
		}
		products = append(products, product)
	}
	return products, nil
}

func main() {
	// Hubungkan ke database
	db, err := connectDB()
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer db.Close()

	// Data untuk dimasukkan
	products := []Product{
		{"Product A", 100.0},
		{"Product B", 150.0},
		{"Product C", 200.0},
		{"Product D", 250.0},
		{"Product E", 300.0},
	}

	var wg sync.WaitGroup
	insertedCount := make(chan int, len(products)) // Channel untuk menghitung jumlah rows yang di-insert

	// Goroutine untuk menangani insert
	for _, product := range products {
		wg.Add(1)
		go func(p Product) {
			defer wg.Done()
			if err := insertProduct(db, p); err != nil {
				log.Printf("Gagal insert product %s: %v", p.Name, err)
			} else {
				log.Printf("Berhasil insert product: %s", p.Name)
				insertedCount <- 1 // Kirim sinyal bahwa 1 row telah di-insert
			}
		}(product)
	}

	// Goroutine untuk memantau perubahan pada channel dan memanggil getProduct
	go func() {
		count := 0
		for {
			select {
			case <-insertedCount:
				count++
				if count%2 == 0 { // Jika sudah menerima 2 rows
					log.Println("Telah memasukkan 2 produk")
					insertedProducts, _ := getProduct(db)
					log.Println("Produk yang telah di-insert:")
					for _, p := range insertedProducts {
						log.Printf("- %s: %.2f", p.Name, p.Price)
					}
				}
			}
		}
	}()

	wg.Wait()            // Tunggu semua goroutine selesai
	close(insertedCount) // Tutup channel setelah semua selesai

	log.Println("Selesai!")
}
