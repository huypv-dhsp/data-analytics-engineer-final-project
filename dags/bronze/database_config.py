POSTGRES_TO_BIGQUERY_ETL_CONFIG = {
    "users": {
        "primary_key": "id",
        "audit_column": "created_at"
    },
    "products": {
        "primary_key": "id",
        "audit_column": "updated_at"
    },
    "orders": {
        "primary_key": "order_id",
        "audit_column": "updated_at"
    },
    "order_items": {
        "primary_key": "id",
        "audit_column": "updated_at"
    },
    "events": {
        "primary_key": "id",
        "audit_column": "created_at"
    },
}
