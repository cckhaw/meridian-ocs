# meridian-ocs chart

Installs the Meridian OCS services. Design, principles, what is rendered, the
verification and the known gaps are in
[docs/architecture/12-deployment.md](../../../docs/architecture/12-deployment.md).

```sh
helm lint deploy/helm/meridian-ocs -f deploy/helm/meridian-ocs/ci/secure.yaml
helm install ocs deploy/helm/meridian-ocs -f my-values.yaml
deploy/helm/test.sh        # lint, render, schema-validate, guards and pod properties
```

Quote `"off"` in values: an unquoted `off` is a YAML boolean and is rejected.
Redis and Kafka are not part of the chart; publish a first catalog version before
expecting the gateway to become ready when `gateway.tariff=catalog`.
