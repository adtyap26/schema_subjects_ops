# Script Explanation:

## Part I: Schema-Topic Mapping
----------------------------
The script begins by fetching Kafka schema subjects and topics. Using these, it maps the relationship between schema subjects (both key and value) and their respective topics. The output of this process is saved in a JSON file named `result_mapping_schema.json` for easy viewing and debugging.

## Part II: Schema Parsing
------------------------
Next, the script processes the mapping results and splits them into two categories:
1. Active schemas: Showing the relationship between schema subjects and topics, saved in `active_schemas.json`.
2. Inactive schemas: Showing schema subjects that are no longer attached to topics (e.g., topics deleted or unattached schemas), saved in `inactive_schemas.txt`.

## Part III: Schema Deletion (Optional)
------------------------------------
In this interactive part, the script checks the `inactive_schemas.txt` file and allows users to choose whether to delete these schemas. Users can opt for:
1. Soft delete: Removes schema versions while retaining metadata, allowing recovery in the future.
2. Hard delete: Permanently removes schema versions and metadata, freeing up space.

Note: Soft deletes retain schema metadata, as explained in the [Confluent documentation](https://docs.confluent.io/platform/current/schema-registry/schema-deletion-guidelines.html#delete-schemas-in-cp).

