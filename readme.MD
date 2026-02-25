## Project Structure

The project follows a clean separation of concerns and is organized as follows:

```
.
analytics-aggregator/
├── cmd/
│   └── api/
│       └── main.go         # Composition root: wires dependencies, starts Chi, handles signals
├── internal/
│   ├── core/               # The "Inside" of the Hexagon
│   │   ├── domain/         # Business entities (Event struct)
│   │   ├── port/           # Interfaces (Repository, External API)
│   │   └── service/        # Business logic (Pipeline, batching, retries)
│   ├── adapter/            # The "Outside" of the Hexagon
│   │   ├── handler/        # Primary/Driving adapter: Chi HTTP routes and handlers
│   │   ├── repository/     # Secondary/Driven adapter: sqlc implementation
│   │   └── extapi/         # Secondary/Driven adapter: Geolocation API client
├── db/                     # Database assets
│   ├── migrations/         # Up/down SQL migration files
│   ├── query.sql           # sqlc queries (e.g., the bulk insert)
│   └── schema.sql          # sqlc schema definition
├── sqlc.yaml               # sqlc configuration file
├── go.mod
└── go.sum
```
