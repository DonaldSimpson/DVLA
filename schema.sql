-- Schema for DVLA MOT Data Import

CREATE DATABASE IF NOT EXISTS mot_data;
USE mot_data;

-- Vehicles table
CREATE TABLE IF NOT EXISTS vehicles (
    registration VARCHAR(20) PRIMARY KEY,
    first_used_date DATETIME,
    registration_date DATETIME,
    manufacture_date DATETIME,
    primary_colour VARCHAR(50),
    secondary_colour VARCHAR(50),
    engine_size INT,
    model VARCHAR(100),
    make VARCHAR(100),
    fuel_type VARCHAR(50),
    last_mot_test_date DATETIME,
    last_update_timestamp VARCHAR(50),
    data_source VARCHAR(50),
    last_update_date DATETIME,
    modification VARCHAR(50)
);

-- MOT Tests table
CREATE TABLE IF NOT EXISTS mot_tests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    registration VARCHAR(20),
    completed_date DATETIME,
    expiry_date DATETIME,
    test_result VARCHAR(50),
    odometer_value INT,
    odometer_unit VARCHAR(20),
    odometer_result_type VARCHAR(50),
    FOREIGN KEY (registration) REFERENCES vehicles(registration)
);

-- Defects table
CREATE TABLE IF NOT EXISTS defects (
    id INT AUTO_INCREMENT PRIMARY KEY,
    mot_test_id INT,
    dangerous BOOLEAN,
    text TEXT,
    type VARCHAR(50),
    FOREIGN KEY (mot_test_id) REFERENCES mot_tests(id)
);

-- Import log table
CREATE TABLE IF NOT EXISTS import_log (
    filename VARCHAR(255) PRIMARY KEY,
    import_timestamp TIMESTAMP,
    status VARCHAR(20)
);

-- Import batch report table
CREATE TABLE IF NOT EXISTS import_batch_report (
    id INT AUTO_INCREMENT PRIMARY KEY,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    duration_seconds INT NOT NULL,
    files_processed INT NOT NULL,
    files_list TEXT NOT NULL,
    total_registrations_added INT NOT NULL,
    total_mot_tests_added INT NOT NULL,
    total_defects_added INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
