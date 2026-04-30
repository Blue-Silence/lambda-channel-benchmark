# Experiment Config Layout

Experiment TOMLs are grouped by workload family and purpose.

```text
blob/
  put/<size>/<backend>.toml
  single-getter/<topology>/<backend>-<size>.toml
  multi-getter/9node/<backend>-<size>.toml
  multi-getter/smoke/<topology>-<backend>-<size>.toml
  microbench/<scenario>.toml
channel/
  single-sender-multi-receiver/smoke/<topology>-<backend>-<mode>-<size>.toml
  single-sender-multi-receiver/3node/<backend>-<mode>-<size>.toml
  single-sender-multi-receiver/9node/<backend>-<mode>-<size>.toml
metadata/
  single-node/<scenario>.toml
```

Workflow defaults should point at these structured paths. Keep smoke configs
separate from formal profile configs so temporary experiments do not get mixed
into the main run matrix.
