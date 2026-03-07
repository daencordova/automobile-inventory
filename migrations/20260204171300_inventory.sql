CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

CREATE DOMAIN domain_car_id AS VARCHAR(10) CHECK (VALUE ~ '^C[0-9]{4,}$');
CREATE DOMAIN domain_warehouse_id AS VARCHAR(20) CHECK (VALUE ~ '^W[0-9]{3,}$');
CREATE DOMAIN domain_customer_id AS VARCHAR(10) CHECK (VALUE ~ '^CU[0-9]{4,}$');
CREATE DOMAIN domain_sale_id AS VARCHAR(10) CHECK (VALUE ~ '^S[0-9]{5,}$');
CREATE DOMAIN domain_email AS VARCHAR(255) CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');
CREATE DOMAIN domain_phone AS VARCHAR(20) CHECK (VALUE ~ '^[0-9\-\+\(\)\s]{7,20}$');

CREATE TYPE enum_car_status AS ENUM ('Available', 'Sold', 'Reserved', 'Maintenance', 'Discontinued');
CREATE TYPE enum_engine_type AS ENUM ('Electric', 'Hybrid', 'Gasoline', 'Diesel', 'Petrol', 'Hydrogen');
CREATE TYPE enum_transmission AS ENUM ('Manual', 'Automatic', 'CVT', 'Semi-Automatic');
CREATE TYPE enum_reservation_status AS ENUM ('Pending', 'Confirmed', 'Expired', 'Cancelled', 'Completed');
CREATE TYPE enum_transfer_status AS ENUM ('Pending', 'InTransit', 'Completed', 'Cancelled', 'Failed');
CREATE TYPE enum_alert_level AS ENUM ('Critical', 'Warning', 'Ok', 'Info');
CREATE TYPE enum_job_status AS ENUM ('Running', 'Completed', 'Failed', 'Cancelled', 'Scheduled');
CREATE TYPE enum_payment_method AS ENUM ('Cash', 'Credit', 'Installment', 'BankTransfer', 'Crypto');
CREATE TYPE enum_gender AS ENUM ('Male', 'Female', 'NonBinary', 'PreferNotToSay');

CREATE TABLE IF NOT EXISTS cars (
    car_id domain_car_id PRIMARY KEY,
    brand VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INT NOT NULL CONSTRAINT check_car_year CHECK (year >= 1886 AND year <= EXTRACT(YEAR FROM CURRENT_DATE) + 1),
    color VARCHAR(30),
    engine_type enum_engine_type NOT NULL DEFAULT 'Gasoline',
    transmission enum_transmission NOT NULL DEFAULT 'Manual',
    price DECIMAL(12, 2) NOT NULL CONSTRAINT check_car_price CHECK (price > 0),
    cost_price DECIMAL(12, 2),
    quantity_in_stock INT DEFAULT 0 CONSTRAINT check_car_qty CHECK (quantity_in_stock >= 0),
    reorder_point INTEGER DEFAULT 5 CONSTRAINT check_reorder_point CHECK (reorder_point >= 0),
    economic_order_qty INTEGER DEFAULT 10 CONSTRAINT check_eoq CHECK (economic_order_qty > 0),
    version BIGINT DEFAULT 1,
    status enum_car_status NOT NULL DEFAULT 'Available',
    search_vector tsvector,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

CREATE TABLE reservations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    car_id VARCHAR(20) NOT NULL REFERENCES cars(car_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    reserved_by VARCHAR(100) NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status reservation_status NOT NULL DEFAULT 'Pending',
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE warehouses (
    warehouse_id VARCHAR(20) PRIMARY KEY CHECK (warehouse_id ~ '^W[0-9]+$'),
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200) NOT NULL,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    capacity_total INTEGER NOT NULL CHECK (capacity_total > 0),
    capacity_used INTEGER DEFAULT 0 CHECK (capacity_used >= 0),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE stock_locations (
    warehouse_id VARCHAR(20) NOT NULL REFERENCES warehouses(warehouse_id),
    car_id VARCHAR(20) NOT NULL REFERENCES cars(car_id),
    zone VARCHAR(20) NOT NULL DEFAULT 'DEFAULT',
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (warehouse_id, car_id)
);

CREATE TABLE transfer_orders (
    transfer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    from_warehouse_id VARCHAR(20) NOT NULL REFERENCES warehouses(warehouse_id),
    to_warehouse_id VARCHAR(20) NOT NULL REFERENCES warehouses(warehouse_id),
    car_id VARCHAR(20) NOT NULL REFERENCES cars(car_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    status transfer_status NOT NULL DEFAULT 'Pending',
    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT check_different_warehouses CHECK (from_warehouse_id != to_warehouse_id)
);

CREATE TABLE IF NOT EXISTS sales_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    car_id VARCHAR(20) NOT NULL REFERENCES cars(car_id),
    quantity INTEGER NOT NULL,
    sale_price DECIMAL(15, 2) NOT NULL,
    customer_id VARCHAR(100),
    sold_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE job_executions (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_type VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status job_status NOT NULL DEFAULT 'Running',
    items_processed INTEGER,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS inventory_metrics_history (
    metric_hour TIMESTAMPTZ PRIMARY KEY,
    total_cars BIGINT NOT NULL,
    total_value NUMERIC(20, 2) NOT NULL,
    active_reservations BIGINT NOT NULL,
    reserved_units BIGINT NOT NULL,
    low_stock_count BIGINT NOT NULL,
    available_stock_value NUMERIC(20, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_cars_active_id ON cars (car_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_cars_deleted_at ON cars (deleted_at);
CREATE INDEX IF NOT EXISTS idx_cars_brand_model ON cars(brand, model);
CREATE INDEX IF NOT EXISTS idx_cars_active_search ON cars(brand, status) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_cars_search_vector ON cars USING GIN(search_vector);

CREATE INDEX idx_reservations_car_id ON reservations(car_id);
CREATE INDEX idx_reservations_status ON reservations(status) WHERE status = 'Pending';
CREATE INDEX idx_reservations_expires_at ON reservations(expires_at) WHERE status = 'Pending';

CREATE INDEX idx_stock_locations_car_id ON stock_locations(car_id);

CREATE INDEX idx_transfers_status ON transfer_orders(status);

CREATE INDEX idx_sales_history_car_id ON sales_history(car_id);
CREATE INDEX idx_sales_history_sold_at ON sales_history(sold_at);

CREATE INDEX IF NOT EXISTS idx_metrics_hour ON inventory_metrics_history(metric_hour DESC);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE OR REPLACE FUNCTION increment_car_version()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.version = OLD.version + 1;
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql';

CREATE OR REPLACE FUNCTION cars_search_vector_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('simple', COALESCE(NEW.brand, '')), 'A') ||
        setweight(to_tsvector('simple', COALESCE(NEW.model, '')), 'B') ||
        setweight(to_tsvector('simple', COALESCE(NEW.color, '')), 'C') ||
        setweight(to_tsvector('simple', COALESCE(NEW.transmission, '')), 'D');
    RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

DROP TRIGGER IF EXISTS cars_search_vector_trigger ON cars;
CREATE TRIGGER cars_search_vector_trigger
    BEFORE INSERT OR UPDATE ON cars
    FOR EACH ROW
    EXECUTE FUNCTION cars_search_vector_update();

CREATE TRIGGER update_cars_modtime
    BEFORE UPDATE ON cars
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER update_reservations_updated_at
    BEFORE UPDATE ON reservations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_increment_car_version
    BEFORE UPDATE ON cars
    FOR EACH ROW
    EXECUTE FUNCTION increment_car_version();

INSERT INTO cars (car_id, brand, model, year, color, engine_type, transmission, price, quantity_in_stock, status)
VALUES
('C0001', 'Toyota', 'Camry', 2023, 'Red', 'Petrol', 'Automatic', 80338.15, 6, 'Available'),
('C0002', 'Tesla', 'Model 3', 2019, 'Red', 'Electric', 'Manual', 26437.73, 16, 'Available'),
('C0003', 'Nissan', 'Qashqai', 2018, 'Blue', 'Electric', 'Automatic', 50158.13, 20, 'Available'),
('C0004', 'Hyundai', 'Sonata', 2025, 'Red', 'Hybrid', 'Automatic', 33026.14, 3, 'Available'),
('C0005', 'Toyota', 'RAV4', 2016, 'White', 'Hybrid', 'Manual', 79672.99, 9, 'Reserved'),
('C0006', 'Hyundai', 'Elantra', 2019, 'White', 'Diesel', 'Automatic', 94225.03, 17, 'Sold'),
('C0007', 'Mercedes', 'C-Class', 2020, 'Gray', 'Petrol', 'Manual', 21344.38, 7, 'Reserved'),
('C0008', 'Tesla', 'Model 3', 2021, 'Blue', 'Electric', 'Manual', 48813.94, 14, 'Reserved'),
('C0009', 'BMW', '3 Series', 2017, 'Red', 'Hybrid', 'Automatic', 34430.1, 14, 'Sold'),
('C0010', 'Tesla', 'Model S', 2015, 'Silver', 'Electric', 'Manual', 42136.55, 0, 'Sold');

INSERT INTO warehouses (warehouse_id, name, location, latitude, longitude, capacity_total, capacity_used)
VALUES
    ('W0001', 'Main Distribution Center', 'Chicago, IL', 41.8781, -87.6298, 15000, 0),
    ('W0002', 'West Coast Fulfillment', 'Oakland, CA', 37.8044, -122.2712, 8000, 0),
    ('W0003', 'Southeast Hub', 'Atlanta, GA', 33.7490, -84.3880, 6000, 0),
    ('W0004', 'Northwest Storage', 'Seattle, WA', 47.6062, -122.3321, 4000, 0),
    ('W0005', 'South Central Depot', 'Houston, TX', 29.7604, -95.3698, 9000, 0),
    ('W0006', 'Mountain Region Center', 'Denver, CO', 39.7392, -104.9903, 5500, 0)
ON CONFLICT (warehouse_id) DO NOTHING;

INSERT INTO stock_locations (warehouse_id, car_id, zone, quantity, reserved_quantity)
VALUES
    ('W0001', 'C0001', 'RECEIVING', 100, 10),
    ('W0001', 'C0002', 'STORAGE-A', 75, 5),
    ('W0001', 'C0003', 'STORAGE-B', 50, 0),
    ('W0001', 'C0004', 'DISPATCH', 25, 25);

UPDATE cars SET search_vector =
    setweight(to_tsvector('simple', COALESCE(brand, '')), 'A') ||
    setweight(to_tsvector('simple', COALESCE(model, '')), 'B') ||
    setweight(to_tsvector('simple', COALESCE(color, '')), 'C') ||
    setweight(to_tsvector('simple', COALESCE(transmission, '')), 'D')
WHERE search_vector IS NULL;
