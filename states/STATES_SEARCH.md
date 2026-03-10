# States Search API

## Endpoint

```
GET /states/search
```

## Description

Search states by name substring and/or state code. Both parameters are case-insensitive.

## Query Parameters

- `name` (string, optional): Substring to search in state names
- `state_code` (string, optional): Exact state code to filter by (2 letters)

At least one parameter must be provided.

## Examples

### Search by name
```bash
GET /states/search?name=new
```

Returns all states with "new" in their name (e.g., New York, New Jersey, New Mexico).

### Search by state code
```bash
GET /states/search?state_code=NY
```

Returns the state with code NY (New York).

### Combined search
```bash
GET /states/search?name=new&state_code=NY
```

Returns states that match both name and state code filters.

## Response Format

```json
{
  "states": {
    "New York": {
      "name": "New York",
      "state_code": "NY",
      "review_count": 0
    },
    "New Jersey": {
      "name": "New Jersey",
      "state_code": "NJ",
      "review_count": 0
    }
  },
  "count": 2
}
```

## Error Response

```json
{
  "error": "Provide at least one parameter: name or state_code"
}
```

Status code: 400

