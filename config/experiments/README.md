# Experiment Config Layout

Experiment TOMLs are grouped by workload family and purpose.

```text
blob/
  put/<size>/<backend>.toml
  multi-getter/9node/<backend>-<size>.toml
  multi-getter/smoke/<topology>-<backend>-<size>.toml
  microbench/<scenario>.toml
metadata/
  single-node/<scenario>.toml
```

Workflow defaults should point at these structured paths. Keep smoke configs
separate from formal profile configs so temporary experiments do not get mixed
into the main run matrix.
