# Cities Search API

## Endpoint

```
GET /cities/search
```

## Description

Search cities by name substring and/or state code. Both parameters are case-insensitive.

## Query Parameters

- `name` (string, optional): Substring to search in city names
- `state_code` (string, optional): Exact state code to filter by

At least one parameter must be provided.

## Examples

### Search by name
```bash
GET /cities/search?name=new
```

Returns all cities with "new" in their name (e.g., New York, New Orleans).

### Search by state
```bash
GET /cities/search?state_code=NY
```

Returns all cities in New York state.

### Combined search
```bash
GET /cities/search?name=spring&state_code=MA
```

Returns cities in Massachusetts with "spring" in their name.

## Response Format

```json
{
  "cities": {
    "New York": {
      "name": "New York",
      "state_code": "NY",
      "review_count": 0
    },
    "New Orleans": {
      "name": "New Orleans",
      "state_code": "LA",
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

