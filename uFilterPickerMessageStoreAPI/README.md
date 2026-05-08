# About

This repository is the API for the [uFilterPickerPickMessageStore](https://github.com/uofuseismo/uFilterPickerMessageStore) - a utility that aggregates and deduplicates uFilterPick pick messages from various instances running the [uFilterPicker](https://github.com/uofuseismo/uFilterPicker) software then makes those picks available to downstream consumers in the UUSS Kubernetes system.  The message store overlays a durable pub/sub that will allow consumers to backfill pick messages for up to a fixed duration (e.g., 15 minutes).
