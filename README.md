# resort-fees

Fetches resort fee data for a list of property IDs from the Booking.com Demand API and writes the results to `executions`
folder in a JSONL format importable to [resort_fees.jsonl](github.com/stayforlong/pricing-engine/blob/master/cmd/syncd/data/taxes/resort_fees.jsonl).

## Requirements

- Go 1.22+

## Install dependencies

This project only uses the Go standard library, so there are no external dependencies to fetch. Just make sure modules are tidy:

```bash
go mod tidy
```

## Setup

1. Copy `.env.template` to `.env` and fill in your credentials:

   ```bash
   cp .env.template .env
   ```

   ```
   BOOKING_TOKEN=<your Booking.com API token>
   BOOKING_AFFILIATE_ID=<your affiliate ID>
   ```

2. Make sure `data/property_ids.csv` contains two IDs per row, the first one with the SFL ID and the second one with the Booking ID. If only Stayforlong IDs are available, you will need to obtain the Booking IDs by querying the `contentdb.mappings` table.

## Run

```bash
go run main.go
```

Optionally limit the number of property IDs processed to process only the first N IDs (debugging purposes):

```bash
go run main.go -max-num-ids 100
```

Results are written as JSONL files under `executions` folder, one file per run.
