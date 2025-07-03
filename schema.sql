-- Schema for DVLA MOT Data Import
-- 
-- Status Column Fix: Updated import_log.status to use shorter values for compatibility
-- STARTED (7) -> Processing has begun
-- READY (5) -> File downloaded and ready for processing  
-- COMPLETED (9) -> File fully processed
-- FAILED (6) -> Processing failed

CREATE DATABASE IF NOT EXISTS mot_data;
-- In PostgreSQL, connect to mot_data manually (psql \c mot_data)

-- Vehicles table
CREATE TABLE IF NOT EXISTS vehicles (
    registration VARCHAR(20) PRIMARY KEY,
    first_used_date TIMESTAMP,
    registration_date TIMESTAMP,
    manufacture_date TIMESTAMP,
    primary_colour VARCHAR(50),
    secondary_colour VARCHAR(50),
    engine_size INT,
    model VARCHAR(100),
    make VARCHAR(100),
    fuel_type VARCHAR(50),
    last_mot_test_date TIMESTAMP,
    last_update_timestamp VARCHAR(50),
    data_source VARCHAR(50),
    last_update_date TIMESTAMP,
    modification VARCHAR(50)
);

-- MOT Tests table
CREATE TABLE IF NOT EXISTS mot_tests (
    id SERIAL PRIMARY KEY,
    registration VARCHAR(20),
    completed_date TIMESTAMP,
    expiry_date TIMESTAMP,
    test_result VARCHAR(50),
    odometer_value INT,
    odometer_unit VARCHAR(20),
    odometer_result_type VARCHAR(50),
);

-- User Audit Log table for tracking site visitors and actions
CREATE TABLE IF NOT EXISTS user_audit_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip_address VARCHAR(45),
    user_agent TEXT,
    referrer TEXT,
    session_id VARCHAR(128),
    endpoint VARCHAR(128),
    method VARCHAR(8),
    request_data TEXT,
    visit_count INT DEFAULT 1,
    status VARCHAR(16) DEFAULT 'ALLOWED'
);
    FOREIGN KEY (registration) REFERENCES vehicles(registration)
);

-- Defects table
CREATE TABLE IF NOT EXISTS defects (
    id SERIAL PRIMARY KEY,
    mot_test_id INT,
    dangerous BOOLEAN,
    text TEXT,
    type VARCHAR(50),
    FOREIGN KEY (mot_test_id) REFERENCES mot_tests(id)
);

-- Import log table
-- Tracks the processing status of downloaded files
-- Status values: STARTED, READY, COMPLETED, FAILED
CREATE UNLOGGED TABLE IF NOT EXISTS import_log (
    filename VARCHAR(255) PRIMARY KEY,
    import_timestamp TIMESTAMP,
    status VARCHAR(20) NOT NULL DEFAULT 'STARTED'
);

-- Migration: Update any existing DOWNLOADED status to READY for compatibility
UPDATE import_log SET status = 'READY' WHERE status = 'DOWNLOADED';

-- Import batch report table
CREATE TABLE IF NOT EXISTS import_batch_report (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_seconds INT NOT NULL,
    files_processed INT NOT NULL,
    files_list TEXT NOT NULL,
    total_registrations_added INT NOT NULL,
    total_mot_tests_added INT NOT NULL,
    total_defects_added INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_mot_tests_completed_date ON mot_tests(completed_date);
CREATE INDEX IF NOT EXISTS idx_defects_mot_test_id ON defects(mot_test_id);
CREATE INDEX IF NOT EXISTS idx_vehicles_make_model ON vehicles(make, model);
