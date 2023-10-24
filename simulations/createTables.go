package simulations

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func handleTransactionErr(err error, tx *sql.Tx) error {
	rollbackErr := tx.Rollback()
	return fmt.Errorf("Failed to create tables. %s. %s", err.Error(), rollbackErr)
}

func PrepDirtyReadTable() (string, error) {
	db := GetDB()

	tx, err := db.Begin()
	if err != nil {
		return "", fmt.Errorf("Couldn't start transaction to run create the tables. %s", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS counters (
			id INT AUTO_INCREMENT PRIMARY KEY,
			counter INT NOT NULL
		);
	`)

	if err != nil {
		return "", handleTransactionErr(err, tx)
	}

	_, err = db.Exec(`
		INSERT INTO counters (id, counter)
		VALUES (1, 2)
		ON DUPLICATE KEY UPDATE
		counter=2;
	`)

	if err != nil {
		return "", handleTransactionErr(err, tx)
	}

	err = tx.Commit()

	if err != nil {
		return "", handleTransactionErr(err, tx)
	}

	return "Created dirty read table", nil

}

func PrepDoctorsTables() (string, error) {
	db := GetDB()

	tx, err := db.Begin()
	if err != nil {
		return "", fmt.Errorf("Couldn't start transaction to run create the tables. %s", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS doctors (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			on_call BOOLEAN NOT NULL
		);
	`)

	if err != nil {
		return "", handleTransactionErr(err, tx)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS shifts (
			id INT AUTO_INCREMENT PRIMARY KEY
		);
	`)

	if err != nil {
		return "", handleTransactionErr(err, tx)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS shifts_doctors (
			shift_id INT NOT NULL,
			doctor_id INT NOT NULL,
			PRIMARY KEY (shift_id, doctor_id),
			FOREIGN KEY (shift_id)
				REFERENCES shifts (id)
				ON DELETE CASCADE,
			FOREIGN KEY (doctor_id)
				REFERENCES doctors (id)
				ON DELETE CASCADE
		);
;
	`)

	if err != nil {
		return "", handleTransactionErr(err, tx)
	}

	err = tx.Commit()

	if err != nil {
		return "", handleTransactionErr(err, tx)
	}

	return "Tables created", nil
}
