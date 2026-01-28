# Medical IoT Data Pipeline

The system must support two distinct use cases:

1. Real-Time Dashboarding: Low-latency lookups (e.g., "Show me the last 1 hour of vitals for Patient X").
2. Long-Term Analytics: Storing historical data for "Septic Shock" prediction models.

## Setup

### Necessary Services

To use the Google Cloud Bigtable Emulator make sure you have Docker installed on your machine and run ([Test using the emulator](https://docs.cloud.google.com/bigtable/docs/emulator#install-docker)):

```
docker run -d -p 127.0.0.1:8086:8086 --name bigtable-emulator google/cloud-sdk gcloud beta emulators bigtable start --host-port=0.0.0.0:8086
```
