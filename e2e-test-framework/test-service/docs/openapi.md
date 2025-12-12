# Drasi Test Service OpenAPI Documentation

The Drasi Test Service now includes comprehensive OpenAPI/Swagger documentation for its REST API.

## Features

- **Interactive API Documentation**: Browse and test the API directly from your browser
- **OpenAPI Specification**: Standards-compliant OpenAPI 3.0 specification
- **Type Safety**: All request/response schemas are automatically generated from Rust types
- **Comprehensive Coverage**: All endpoints are documented with parameters, request bodies, and response schemas

## Accessing the Documentation

When the test service is running, you can access:

### Interactive Documentation (Swagger UI)
- **URL**: `http://localhost:63123/docs`
- **Description**: Interactive web interface for exploring and testing the API
- **Features**: 
  - Try out API endpoints directly
  - View request/response schemas
  - See example values
  - Authentication testing (if applicable)

### OpenAPI JSON Specification  
- **URL**: `http://localhost:63123/api-docs/openapi.json`
- **Description**: Raw OpenAPI 3.0 specification in JSON format
- **Use Cases**:
  - Generate client SDKs
  - Import into tools like Postman or Insomnia
  - API testing and validation
  - Integration with CI/CD pipelines

## API Structure

The API is organized into the following main categories:

### Service (`/`)
- `GET /` - Get service information and status

### Test Repositories (`/test_repos`)
- `GET /test_repos` - List all repositories
- `POST /test_repos` - Create a new repository
- `GET /test_repos/{repo_id}` - Get repository details
- `GET /test_repos/{repo_id}/tests` - List tests in repository
- `POST /test_repos/{repo_id}/tests` - Add a test to repository
- `GET /test_repos/{repo_id}/tests/{test_id}` - Get test details
- `GET /test_repos/{repo_id}/tests/{test_id}/sources` - List test sources
- `POST /test_repos/{repo_id}/tests/{test_id}/sources` - Add a test source
- `GET /test_repos/{repo_id}/tests/{test_id}/sources/{source_id}` - Get source details

### Test Sources (`/test_run_host/sources`)
- `GET /test_run_host/sources` - List all sources
- `POST /test_run_host/sources` - Create a new source
- `GET /test_run_host/sources/{id}` - Get source state
- `POST /test_run_host/sources/{id}/bootstrap` - Get bootstrap data
- `POST /test_run_host/sources/{id}/start` - Start source
- `POST /test_run_host/sources/{id}/stop` - Stop source
- `POST /test_run_host/sources/{id}/pause` - Pause source
- `POST /test_run_host/sources/{id}/reset` - Reset source
- `POST /test_run_host/sources/{id}/skip` - Skip events
- `POST /test_run_host/sources/{id}/step` - Step through events

### Test Queries (`/test_run_host/queries`)
- `GET /test_run_host/queries` - List all queries
- `POST /test_run_host/queries` - Create a new query
- `GET /test_run_host/queries/{id}` - Get query state
- `GET /test_run_host/queries/{id}/profile` - Get query result profile (HTML)
- `POST /test_run_host/queries/{id}/start` - Start query observer
- `POST /test_run_host/queries/{id}/stop` - Stop query observer
- `POST /test_run_host/queries/{id}/pause` - Pause query observer
- `POST /test_run_host/queries/{id}/reset` - Reset query observer

## Schema Documentation

All API endpoints include detailed schema documentation for:

- **Request Parameters**: Path parameters, query parameters, and request bodies
- **Response Schemas**: Detailed response structure with field descriptions
- **Error Responses**: Standard error responses with appropriate HTTP status codes
- **Data Models**: Complete schemas for all data types used in the API

## Getting Started

1. **Start the Test Service**:
   ```bash
   cargo run --bin test-service
   ```

2. **Open the Documentation**:
   Navigate to `http://localhost:63123/docs` in your browser

3. **Explore the API**:
   - Browse the available endpoints
   - Try out operations directly from the browser
   - View example request/response data

4. **Generate Client Code** (Optional):
   - Download the OpenAPI spec from `http://localhost:63123/api-docs/openapi.json`
   - Use tools like `openapi-generator` to create client SDKs in your preferred language

## Development Notes

- All OpenAPI annotations are maintained alongside the Rust code using `utoipa` macros
- Schema generation is automatic from Rust types using `#[derive(ToSchema)]`
- API paths are documented using `#[utoipa::path()]` attributes
- The documentation stays in sync with the code automatically

For more details on extending or modifying the API documentation, see the `utoipa` documentation at [docs.rs/utoipa](https://docs.rs/utoipa/).
