CREATE EXTENSION "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "btree_gin";

CREATE DOMAIN warehouse_id AS VARCHAR(20);

CREATE TYPE car_status AS ENUM ('Available', 'Sold', 'Reserved', 'Maintenance');
CREATE TYPE engine_type AS ENUM ('Electric', 'Hybrid', 'Gasoline', 'Diesel', 'Petrol');
CREATE TYPE reservation_status AS ENUM ('Pending', 'Confirmed', 'Expired', 'Cancelled', 'Completed');
CREATE TYPE transfer_status AS ENUM ('Pending', 'InTransit', 'Completed', 'Cancelled');
CREATE TYPE alert_level AS ENUM ('Critical', 'Warning', 'Ok');
CREATE TYPE job_status AS ENUM ('Running', 'Completed', 'Failed');
CREATE TYPE payment_method AS ENUM ('Cash', 'Credit', 'Installment', 'Debit', 'BankTransfer');
CREATE TYPE gender AS ENUM ('Male', 'Female', 'Other');

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

CREATE TABLE cars (
    car_id VARCHAR(10) PRIMARY KEY,
    brand VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INT NOT NULL CONSTRAINT check_year CHECK (year >= 1886 AND year <= EXTRACT(YEAR FROM CURRENT_DATE) + 1),
    color VARCHAR(30),
    engine_type engine_type NOT NULL,
    transmission VARCHAR(20),
    price DECIMAL(12, 2) NOT NULL CONSTRAINT check_price CHECK (price > 0),
    quantity_in_stock INT DEFAULT 0 CONSTRAINT check_qty CHECK (quantity_in_stock >= 0),
    reorder_point INTEGER DEFAULT 5 CHECK (reorder_point >= 0),
    economic_order_qty INTEGER DEFAULT 10 CHECK (economic_order_qty > 0),
    version BIGINT DEFAULT 1,
    status car_status NOT NULL DEFAULT 'Available',
    search_vector tsvector,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE TABLE customers (
    customer_id VARCHAR(10) PRIMARY KEY CHECK (customer_id ~ '^CU[0-9]+$'),
    name VARCHAR(100) NOT NULL,
    gender gender NOT NULL,
    age INTEGER NOT NULL CHECK (age >= 18 AND age <= 120),
    phone VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE sales (
    sale_id VARCHAR(10) PRIMARY KEY CHECK (sale_id ~ '^S[0-9]+$'),
    customer_id VARCHAR(10) NOT NULL REFERENCES customers(customer_id),
    car_id VARCHAR(10) NOT NULL REFERENCES cars(car_id),
    sale_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    sale_price DECIMAL(15, 2) NOT NULL CHECK (sale_price > 0),
    payment_method VARCHAR(20) NOT NULL CHECK (payment_method IN ('Cash', 'Credit', 'Installment')),
    salesperson VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
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

CREATE TABLE job_executions (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_type VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status job_status NOT NULL DEFAULT 'Running',
    items_processed INTEGER,
    error_message TEXT
);

CREATE TABLE inventory_metrics_history (
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
CREATE INDEX idx_cars_brand_model ON cars(brand, model);
CREATE INDEX idx_cars_active_search ON cars(brand, status) WHERE deleted_at IS NULL;
CREATE INDEX idx_cars_search_vector ON cars USING GIN(search_vector);
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_city ON customers(city);
CREATE INDEX idx_customers_active ON customers(is_active) WHERE is_active = true;
CREATE INDEX idx_sales_customer_id ON sales(customer_id);
CREATE INDEX idx_sales_car_id ON sales(car_id);
CREATE INDEX idx_sales_date ON sales(sale_date DESC);
CREATE INDEX idx_sales_payment_method ON sales(payment_method);
CREATE INDEX idx_reservations_car_id ON reservations(car_id);
CREATE INDEX idx_reservations_status ON reservations(status) WHERE status = 'Pending';
CREATE INDEX idx_reservations_expires_at ON reservations(expires_at) WHERE status = 'Pending';
CREATE INDEX idx_stock_locations_car_id ON stock_locations(car_id);
CREATE INDEX idx_transfers_status ON transfer_orders(status);
CREATE INDEX idx_metrics_hour ON inventory_metrics_history(metric_hour DESC);

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

CREATE TRIGGER cars_search_vector_trigger
    BEFORE INSERT OR UPDATE ON cars
    FOR EACH ROW
    EXECUTE FUNCTION cars_search_vector_update();

CREATE TRIGGER update_cars_modtime
    BEFORE UPDATE ON cars
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_at_column();

CREATE TRIGGER trigger_increment_car_version
    BEFORE UPDATE ON cars
    FOR EACH ROW
    EXECUTE FUNCTION increment_car_version();

CREATE TRIGGER update_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sales_updated_at
    BEFORE UPDATE ON sales
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_reservations_updated_at
    BEFORE UPDATE ON reservations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

INSERT INTO warehouses (warehouse_id, name, location, latitude, longitude, capacity_total, capacity_used)
VALUES
    ('W00001', 'Main Distribution Center', 'Chicago, IL', 41.8781, -87.6298, 15000, 0),
    ('W00002', 'West Coast Fulfillment', 'Oakland, CA', 37.8044, -122.2712, 8000, 0),
    ('W00003', 'Southeast Hub', 'Atlanta, GA', 33.7490, -84.3880, 6000, 0),
    ('W00004', 'Northwest Storage', 'Seattle, WA', 47.6062, -122.3321, 4000, 0),
    ('W00005', 'South Central Depot', 'Houston, TX', 29.7604, -95.3698, 9000, 0),
    ('W00006', 'Mountain Region Center', 'Denver, CO', 39.7392, -104.9903, 5500, 0)
ON CONFLICT (warehouse_id) DO NOTHING;

INSERT INTO cars (car_id, brand, model, year, color, engine_type, transmission, price, quantity_in_stock, status)
VALUES
    ('C00001', 'Toyota', 'Camry', 2023, 'Red', 'Petrol', 'Automatic', 80338.15, 6, 'Available'),
    ('C00002', 'Tesla', 'Model 3', 2019, 'Red', 'Electric', 'Manual', 26437.73, 16, 'Available'),
    ('C00003', 'Nissan', 'Qashqai', 2018, 'Blue', 'Electric', 'Automatic', 50158.13, 20, 'Available'),
    ('C00004', 'Hyundai', 'Sonata', 2025, 'Red', 'Hybrid', 'Automatic', 33026.14, 3, 'Available'),
    ('C00005', 'Toyota', 'RAV4', 2016, 'White', 'Hybrid', 'Manual', 79672.99, 9, 'Reserved'),
    ('C00006', 'Hyundai', 'Elantra', 2019, 'White', 'Diesel', 'Automatic', 94225.03, 17, 'Sold'),
    ('C00007', 'Mercedes', 'C-Class', 2020, 'Gray', 'Petrol', 'Manual', 21344.38, 7, 'Reserved'),
    ('C00008', 'Tesla', 'Model 3', 2021, 'Blue', 'Electric', 'Manual', 48813.94, 14, 'Reserved'),
    ('C00009', 'BMW', '3 Series', 2017, 'Red', 'Hybrid', 'Automatic', 34430.1, 14, 'Sold'),
    ('C00010', 'Tesla', 'Model S', 2015, 'Silver', 'Electric', 'Manual', 42136.55, 0, 'Sold');

INSERT INTO customers (customer_id, name, gender, age, phone, email, city)
VALUES
    ('CU0001', 'Jill Snyder', 'Male', 49, '(527)170-6357', 'timothy92@yahoo.com', 'Juliabury'),
    ('CU0002', 'Nicholas Foster', 'Male', 23, '324-959-9856x281', 'cochrancarlos@berry.info', 'Wrightport'),
    ('CU0003', 'Courtney Robbins', 'Male', 60, '461.428.6407', 'donna01@yahoo.com', 'Lake Zacharyburgh'),
    ('CU0004', 'Blake Barry', 'Male', 67, '119-340-1448x571', 'sandra08@yahoo.com', 'Ramirezburgh'),
    ('CU0005', 'Claudia Hardin', 'Female', 31, '514-684-2974x378', 'caitlindavis@bradley.org', 'Lake Tamara'),
    ('CU0006', 'Scott Wilson', 'Female', 52, '(356)146-5556x78074', 'pamela97@smith.net', 'East Amberland'),
    ('CU0007', 'Michael Johnson', 'Male', 58, '(272)146-6686x12407', 'rjones@bates.com', 'Holmesport'),
    ('CU0008', 'Michelle Davis', 'Female', 28, '061.225.2031', 'dwilson@schmidt-carter.com', 'New Robin'),
    ('CU0009', 'Sara Mack', 'Male', 29, '558.988.9694x89389', 'ybailey@yahoo.com', 'East Brian'),
    ('CU0010', 'Ashley Butler', 'Female', 30, '874-636-8430', 'williamestes@gmail.com', 'North Marciamouth'),
    ('CU0011', 'Charles Sullivan', 'Male', 65, '+1-766-952-6723x886', 'fward@pierce.com', 'South Adam'),
    ('CU0012', 'Tiffany Thompson', 'Male', 30, '7124538481', 'lindseychad@mckinney-cole.com', 'Port Ronaldberg'),
    ('CU0013', 'Juan Blackwell', 'Male', 23, '7553482919', 'dguerrero@gmail.com', 'North Jessica'),
    ('CU0014', 'Todd Taylor', 'Female', 53, '+1-994-176-8255x022', 'rodriguezangela@mills-navarro.net', 'New Susantown'),
    ('CU0015', 'Brooke Bennett', 'Male', 23, '477.655.4719', 'marvin28@hotmail.com', 'East Jack')
ON CONFLICT (customer_id) DO NOTHING;

INSERT INTO sales (
    sale_id, customer_id, car_id, warehouse_id, sale_date, quantity, sale_price, payment_method, salesperson
) VALUES
    ('S00001', 'CU0001', 'C00003', 'W00001', '2024-02-12', 3, 73293.19, 'Installment', 'Ashley Ramos'),
    ('S00002', 'CU0001', 'C00004', 'W00002', '2024-02-12', 3, 32681.20, 'Cash', 'Pamela Blair'),
    ('S00003', 'CU0002', 'C00003', 'W00001', '2024-02-12', 2, 53530.92, 'Credit', 'Sergio Lee'),
    ('S00004', 'CU0003', 'C00004', 'W00003', '2024-02-12', 1, 89816.61, 'Cash', 'Mary Johnston'),
    ('S00005', 'CU0004', 'C00005', 'W00002', '2024-02-12', 2, 77590.86, 'Installment', 'Ricardo Garcia'),
    ('S00006', 'CU0005', 'C00006', 'W00001', '2024-02-12', 2, 79567.24, 'Cash', 'Chelsea Gonzalez'),
    ('S00007', 'CU0006', 'C00007', 'W00004', '2024-02-12', 2, 65470.00, 'Credit', 'Carol Lopez'),
    ('S00008', 'CU0007', 'C00008', 'W00002', '2024-02-12', 3, 98436.54, 'Cash', 'Alexa Edwards'),
    ('S00009', 'CU0008', 'C00009', 'W00003', '2024-02-12', 1, 79282.57, 'Installment', 'Joe Goodwin'),
    ('S00010', 'CU0009', 'C00010', 'W00001', '2024-02-12', 3, 59004.55, 'Installment', 'Chase Harrison'),
    ('S00011', 'CU0010', 'C00003', 'W00005', '2024-02-12', 2, 60687.78, 'Credit', 'Molly Charles'),
    ('S00012', 'CU0011', 'C00001', 'W00002', '2024-02-12', 3, 60037.64, 'Cash', 'John Kemp'),
    ('S00013', 'CU0012', 'C00002', 'W00001', '2024-02-12', 3, 46668.56, 'Installment', 'Brian Austin'),
    ('S00014', 'CU0013', 'C00006', 'W00004', '2024-02-12', 1, 34807.99, 'Credit', 'Ronald Harmon'),
    ('S00015', 'CU0014', 'C00009', 'W00003', '2024-02-12', 3, 18527.23, 'Credit', 'Dr. Lee Davis PhD'),
    ('S00016', 'CU0015', 'C00010', 'W00005', '2024-02-12', 1, 15500.29, 'Installment', 'Michael Hernandez')
ON CONFLICT (sale_id) DO NOTHING;

INSERT INTO stock_locations (warehouse_id, car_id, zone, quantity, reserved_quantity)
VALUES
    ('W00001', 'C00001', 'RECEIVING', 100, 10),
    ('W00001', 'C00002', 'STORAGE-A', 75, 5),
    ('W00001', 'C00003', 'STORAGE-B', 50, 0),
    ('W00001', 'C00004', 'DISPATCH', 25, 25);

UPDATE cars SET search_vector =
    setweight(to_tsvector('simple', COALESCE(brand, '')), 'A') ||
    setweight(to_tsvector('simple', COALESCE(model, '')), 'B') ||
    setweight(to_tsvector('simple', COALESCE(color, '')), 'C') ||
    setweight(to_tsvector('simple', COALESCE(transmission, '')), 'D')
WHERE search_vector IS NULL;

CREATE OR REPLACE VIEW sales_summary AS
SELECT
    s.sale_id,
    s.sale_date,
    c.customer_id,
    c.name as customer_name,
    c.city as customer_city,
    car.brand,
    car.model,
    car.car_id,
    s.quantity,
    s.sale_price,
    s.sale_price * s.quantity as total_amount,
    s.payment_method,
    s.salesperson,
    w.name as warehouse_name
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
JOIN cars car ON s.car_id = car.car_id
LEFT JOIN warehouses w ON s.warehouse_id = w.warehouse_id
WHERE c.deleted_at IS NULL;

CREATE OR REPLACE VIEW customer_metrics AS
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.city,
    COUNT(s.sale_id) as total_purchases,
    SUM(s.quantity) as total_units_bought,
    SUM(s.sale_price * s.quantity) as lifetime_value,
    MAX(s.sale_date) as last_purchase_date,
    CASE
        WHEN MAX(s.sale_date) > NOW() - INTERVAL '90 days' THEN 'Active'
        WHEN MAX(s.sale_date) > NOW() - INTERVAL '365 days' THEN 'Dormant'
        ELSE 'Inactive'
    END as customer_status
FROM customers c
LEFT JOIN sales s ON c.customer_id = s.customer_id
WHERE c.deleted_at IS NULL
GROUP BY c.customer_id, c.name, c.email, c.city;
