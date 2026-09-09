# Airbyte Destination: Directus

Custom Docker destination that writes Airbyte records into Directus collections.

Image: `ghcr.io/itsablabla/airbyte-destination-directus:0.1.0`

## Config
- `url`: Directus base URL
- `token`: static/admin token (preferred)
- or `email` + `password`
- optional `collection_map` / `primary_key_map` for Beeper streams

## Default Beeper mapping
| Stream | Collection | Upsert key |
|--------|------------|------------|
| accounts | beeper_accounts | account_id |
| bridges | beeper_bridges | bridge_id |
| chats / chats_search | beeper_chats | chat_id |
| messages_search | beeper_messages | message_id |
| contacts | beeper_contacts | contact_key |
