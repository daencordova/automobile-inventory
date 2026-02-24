# Automobile Inventory System

[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![Axum](https://img.shields.io/badge/framework-axum-blue.svg)](https://github.com/tokio-rs/axum)
[![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A high-performance, asynchronous REST API engineered with **Rust**, **Axum**, and **PostgreSQL**. This project implements a production-ready **Hexagonal Architecture** (Ports & Adapters) designed for scalability, type safety, and observability in automotive inventory management.

&gt; **Architecture Pattern:** Clean Architecture / Hexagonal Architecture with clear separation between Domain, Application, and Infrastructure layers.

---

## ✨ Key Features

### Core Infrastructure
* **Asynchronous Runtime:** Powered by `tokio` for non-blocking I/O and maximum throughput.
* **Hexagonal Architecture:** Strict separation of concerns between Domain Models, Application Services (Business Logic), and Infrastructure (Repositories, HTTP handlers).
* **Type-Safe Persistence:** `SQLx` provides compile-time checked SQL queries via macros, ensuring database schema and code are always synchronized.
* **Zero-Cost Abstractions:** Leveraging Rust's ownership model for memory safety without garbage collection.

### Advanced Inventory Management
* **Smart Reservations:** TTL-based stock reservations with automatic expiration and conflict detection.
* **Multi-Warehouse Support:** Distributed inventory across locations with intelligent stock transfers.
* **Predictive Analytics:** Sales velocity tracking, depreciation analysis, and automated low-stock alerts.
* **Optimistic Concurrency:** Version-based conflict resolution for concurrent inventory updates.

### Production-Grade Middleware & Observability
* **Security & Resilience:**
  * **Rate Limiting:** IP-based request throttling via `tower-governor`.
  * **CORS:** Fully configurable per environment (development/staging/production).
  * **Request Timeouts:** Global timeout protection with graceful degradation.
  * **Payload Limits:** Protection against oversized request bodies (2MB default).
* **Observability:**
  * **Structured Logging:** JSON-formatted logs in production, pretty logs in development via `tracing`.
  * **Distributed Tracing:** Automatic `X-Request-ID` propagation for request correlation.
  * **Performance Metrics:** Response time tracking and database health monitoring.
* **Graceful Shutdown:** Proper handling of `SIGTERM` and `SIGINT` for zero-downtime deployments.

---

## 🗂️ Inventory Domain Model

The system manages a comprehensive automobile inventory with support for complex business operations:

| Category | Attributes |
|----------|-----------|
| **Core Specifications** | Brand, Model, Year (1886–2026), Color, VIN |
| **Mechanical Data** | Engine Type (Electric, Hybrid, Gasoline, Diesel, Petrol), Transmission |
| **Inventory Logic** | Real-time quantity tracking, Status (Available, Sold, Reserved, Maintenance), Multi-warehouse allocation |
| **Financials** | High-precision decimal pricing (BigDecimal), Depreciation tracking |
| **Operational** | Reorder points, Economic order quantities, Stock alerts |

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Rust 1.75+ | Type-safe systems programming |
| **Web Framework** | Axum 0.7 | Modular, tower-based HTTP framework |
| **Database** | PostgreSQL 15+ | ACID-compliant relational storage |
| **ORM/Query Builder** | SQLx 0.7 | Compile-time checked SQL |
| **Serialization** | Serde | JSON serialization/deserialization |
| **Validation** | Validator | Declarative input validation |
| **API Documentation** | Utoipa 4.x | OpenAPI/Swagger generation |
| **Configuration** | config-rs | Hierarchical environment configuration |
| **Secrets Management** | secrecy | Secure handling of credentials |

---

## 📚 API Documentation

Interactive API documentation is automatically generated and available when the server is running:

📌 **Swagger UI:** `http://localhost:3000/swagger-ui`  
📌 **OpenAPI Spec:** `http://localhost:3000/api-docs/openapi.json`

### Core Endpoints Overview

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | System health check (database connectivity, uptime) |
| `POST` | `/api/v1/cars` | Create new automobile entry |
| `GET` | `/api/v1/cars` | Paginated list with filtering (brand, status, year) |
| `GET` | `/api/v1/cars/{id}` | Retrieve specific vehicle details |
| `PUT` | `/api/v1/cars/{id}` | Full update of vehicle data |
| `PUT` | `/api/v1/cars/{id}/versioned` | Optimistic concurrency update |
| `DELETE` | `/api/v1/cars/{id}` | Soft delete vehicle |
| `POST` | `/api/v1/cars/{id}/reservations` | Create stock reservation |
| `GET` | `/api/v1/warehouses` | List all warehouses |
| `POST` | `/api/v1/warehouses/transfers` | Initiate stock transfer |
| `GET` | `/api/v1/inventory/alerts` | Critical stock alerts |
| `GET` | `/api/v1/inventory/metrics` | Dashboard KPIs |

---

## 🚀 Getting Started

### Prerequisites

Ensure you have the following installed:

* **Rust:** Latest stable toolchain via [rustup](https://rustup.rs/)
  ```bash
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  
PostgreSQL: Version 15 or higher (local installation or Docker)
SQLx CLI: Database migration and query preparation tool

```bash
cargo install sqlx-cli --no-default-features --features native-tls,postgres
```

### Installation

1. **Clone & Setup:**
```bash
git clone [https://github.com/daencordova/automobile-inventory.git](https://github.com/daencordova/automobile-inventory.git)
cd automobile-inventory
cp .env.example .env
```

#### Project Structure

```
automobile-inventory/
├── src/
│   ├── config.rs          # Environment configuration & validation
│   ├── error.rs           # Centralized error handling & HTTP mapping
│   ├── extractors.rs      # Custom Axum extractors (ValidatedJson)
│   ├── handlers.rs        # HTTP route handlers (primary adapters)
│   ├── lib.rs             # Library exports
│   ├── main.rs            # Application entry point
│   ├── middleware.rs      # Request context & logging middleware
│   ├── models.rs          # Domain entities, DTOs, value objects
│   ├── observability.rs   # Tracing & metrics initialization
│   ├── repositories.rs    # Database access layer (secondary adapters)
│   ├── routes.rs          # Router composition & middleware stack
│   ├── services.rs        # Business logic & use cases
│   ├── state.rs           # Application state management
│   └── telemetry.rs       # Additional telemetry utilities
├── migrations/            # Versioned SQL schema changes
├── config/                # Hierarchical configuration files
│   ├── default.yaml
│   ├── development.yaml
│   └── production.yaml
├── Cargo.toml
├── .env.example
└── README.md
```

2.  **Configure Environment:**

Update the data for file named `.env` in the project root:

```env
# Application
APP_ENVIRONMENT=development
RUST_LOG=debug

# Database
DATABASE_URL=postgres://postgres:postgres@localhost:5432/automobile_inventory
```

3. **Database Initialization:**

Use the SQLx CLI to set up your database and run all migrations.

```bash
# Create database (if it doesn't exist)
sqlx database create

# Run migrations
sqlx migrate run

# Verify schema
sqlx migrate info
```
  
4. **Prepare SQLx for Offline Building (Recommended):**

Generate the `sqlx-data.json` file which caches query checks, allowing you to compile without a live database connection.

```bash
cargo sqlx prepare
```

5. **Running the App:**

To start the server:

```bash
# Development mode with hot reload (requires cargo-watch)
cargo watch -x run

# Or standard run
cargo run

# Production build
cargo build --release
./target/release/automobile-inventory
```

The server will be available at http://localhost:3000/swagger-ui/

## Running Tests:

Run this command on terminal:

```bash
# Run all integration tests
cargo test --test '*'

# Run specific test suite with output
cargo test --test car_tests -- --nocapture

# Run with debug logging
RUST_LOG=debug cargo test -- --nocapture
```
