# Black Diamond Timeline Configuration - Organization org_123456789012

## Overview

This directory contains the canonical timeline event configuration for Black Diamond wealth management data for organization **org_123456789012**. It implements **Phase 1** of the Black Diamond event timeline (12 core events).

## Structure

```
org_123456789012/
├── README.md                                    # This file
├── dag.yaml                                     # DAG and timeline configuration
└── sources/                                     # Source event definitions
    ├── blackdiamond_relationships.yaml          # 1 event:  relationship_created
    ├── blackdiamond_contacts.yaml               # 1 event:  contact_created
    ├── blackdiamond_portfolios.yaml             # 1 event:  portfolio_created
    ├── blackdiamond_accounts.yaml               # 2 events: account_created, account_opened
    ├── blackdiamond_portfolio_accounts.yaml     # 1 event:  account_assigned_to_portfolio
    ├── blackdiamond_holdings.yaml               # 5 events: holding_established, transaction_buy/sell/deposit/withdrawal
    └── blackdiamond_tax_lots.yaml               # 1 event:  tax_lot_opened
```

## Phase 1 Events (12 Total)

| Event Type | Source Table | Entity Type | Timestamp Field | Status |
|------------|--------------|-------------|-----------------|--------|
| relationship_created | relationships | relationship | inception_date | ✅ Config |
| contact_created | contacts | contact | derived | ✅ Config |
| portfolio_created | portfolios | portfolio | derived | ✅ Config |
| account_created | accounts | account | create_date | ✅ Config |
| account_opened | accounts | account | start_date | ✅ Config |
| account_assigned_to_portfolio | portfolio_accounts | account | derived | ✅ Config |
| transaction_buy | holdings | holding | as_of_date | ✅ Config |
| transaction_sell | holdings | holding | as_of_date | ✅ Config |
| transaction_deposit | holdings | holding | as_of_date | ✅ Config |
| transaction_withdrawal | holdings | holding | as_of_date | ✅ Config |
| holding_established | holdings | holding | as_of_date | ✅ Config |
| tax_lot_opened | tax_lots | tax_lot | open_date | ✅ Config |

## Setup Instructions

### 1. Copy for Your Organization

```bash
# Copy this template directory
cp -r timeline/org_123456789012 timeline/org_{YOUR_ORG_ID}

# Example:
cp -r timeline/org_123456789012 timeline/org_29a436a3_b5de_4afd_9c7a_059246c5a681
```

### 2. Update Configuration

Edit `dag.yaml` and update:

```yaml
organization:
  org_id: "YOUR-ORG-UUID"              # Your organization UUID
  org_name: "Your Organization Name"
  database: "org_your_org_id"          # Your org ClickHouse database
```

### 3. Prerequisites

Ensure these Silver tables exist in ClickHouse:

- `blackdiamond_silver.relationships` (from `relationships_raw`)
- `blackdiamond_silver.contacts` (from `contacts_raw`)
- `blackdiamond_silver.portfolios` (from `portfolios_raw`)
- `blackdiamond_silver.accounts` (from `accounts_raw`)
- `blackdiamond_silver.holdings` (from `holdings_raw`)
- `blackdiamond_silver.tax_lots` (from `tax_lots_raw`)
- `blackdiamond_silver.portfolio_accounts` (from `portfolio_accounts_raw`)

**NOTE:** If your Silver tables have different names (e.g., `portfolio_events` instead of `portfolios`), update the `source.table` field in each source YAML file.

### 4. Deploy to MinIO

Upload the configuration to MinIO:

```bash
# Upload to MinIO airflow-configs bucket
mc cp -r timeline/org_your_org_id/ \
  minio/airflow-configs/timeline/org_your_org_id/
```

### 5. Trigger the DAG

The DAG will:
1. Create the timeline database and table
2. Create 7 materialized views (one per source)
3. Backfill existing data
4. Validate the timeline

```bash
airflow dags trigger timeline__org_your_org_id
```

## Important Notes

### Transaction Events Limitation

Black Diamond's `holdings` table represents **positions/snapshots**, not individual transactions. The transaction events (buy/sell/deposit/withdrawal) are **INFERRED** from holding snapshots:

- `transaction_buy`: Holdings with positive units (non-cash)
- `transaction_sell`: Holdings with negative units
- `transaction_deposit`: Cash holdings with positive value
- `transaction_withdrawal`: Cash holdings with negative value

**This is NOT the same as true transaction records!** For accurate transaction history, Black Diamond would need to provide explicit transaction data, or we would need to implement historical diff logic.

### Derived Timestamps

Several events use "derived" timestamps because Black Diamond doesn't store explicit event dates:

- `contact_created`: Uses `processing_date` (should ideally derive from relationship inception)
- `portfolio_created`: Uses `processing_date` (should ideally derive from first account)
- `account_assigned_to_portfolio`: Uses `processing_date` (should ideally be later of portfolio/account dates)

These can be improved in Phase 2 with JOIN logic to derive more accurate timestamps.

## Phase 2 & 3 Events (Future)

The following events are defined in the documentation but NOT YET IMPLEMENTED:

### Phase 2 (12 High-Value Events)
- account_closed
- account_reconciled
- goal_created
- goal_assigned_to_account
- target_created
- transaction_dividend
- transaction_interest
- transaction_contribution
- portfolio_benchmark_assigned
- advisor_assigned_to_portfolio
- tax_lot_closed
- holding_closed

### Phase 3 (12 Enhanced Events)
- relationship_terminated
- contact_assigned_primary
- contact_assigned_secondary
- billing_period_started
- billing_period_ended
- target_deactivated
- transaction_employer_match
- transaction_transfer
- transaction_grant
- portfolio_benchmark_removed
- team_assigned_to_relationship
- portfolio_group_assigned

See `Canonical_TimelineEventDefinition_BlackDiamond.md` for full event specifications.

## Monitoring

After deployment, check timeline health:

```sql
-- Event count by type
SELECT event_type, count() as cnt
FROM org_your_org_id.timeline
GROUP BY event_type
ORDER BY cnt DESC;

-- Event count by entity type
SELECT entity_type, count() as cnt
FROM org_your_org_id.timeline
GROUP BY entity_type
ORDER BY cnt DESC;

-- Recent events
SELECT *
FROM org_your_org_id.timeline
ORDER BY timestamp DESC
LIMIT 100;
```

## Troubleshooting

### No events appearing
- Check that Silver tables exist and have data
- Verify `glynac_organization_id` matches your org_id
- Check materialized view status: `SHOW TABLES IN org_your_org_id LIKE 'mv_%'`

### Missing metadata fields
- Review source YAML `metadata` sections
- Ensure referenced columns exist in Silver tables

### Duplicate events
- Check `version_column` in timeline table (uses `ReplacingMergeTree`)
- Verify event_id uniqueness

## References

- **Event Definitions**: `/ALL-Docs/canonical/Canonical_TimelineEventDefinition_BlackDiamond.md`
- **Configuration Guide**: `/ALL-Docs/canonical/02-CONFIGURATION-GUIDE.md`
- **Timeline Handler**: `/ALL-Docs/canonical/05-TIMELINE-HANDLER.md`
