# s3d Helm chart

Runs one `s3d` instance as a StatefulSet with a persistent data volume.

```sh
helm install s3d oci://ghcr.io/siafoundation/charts/s3d \
  --set adminPassword=<password> \
  --set recoveryPhrase="<12 words>" \
  --set preauthorizedKey=<hex>
```

The packaged chart carries the release version, so the image tag follows it.
When installing from a checkout set `image.tag`, the `appVersion` there is a
placeholder.

On the first start the pod registers with the indexer using the pre-authorized
key and the recovery phrase, then stores the app key on the data volume. Later
starts use the stored key and no longer read the phrase or the pre-authorized
key, so both can be removed from the values once the pod has come up. Helm
keeps earlier revisions around, so they stay readable through the release
history until it is pruned or the release is uninstalled. The key is minted on
the indexer, expires and is good for a limited number of uses.

The data volume holds the metadata database and the objects waiting to be
uploaded to Sia. It is not deleted by `helm uninstall`, a reinstall with the
same release name picks it up again, and `persistence.size` should cover
`sia.diskUsageLimit` in the config, 10 GiB by default.

Run a single instance per app key. The chart pins the replica count to one for
that reason, see the project README.

## Values

| Value | Description |
|-------|-------------|
| `adminPassword` | Password of the admin API, required |
| `indexerURL` | Indexer to register with (default `https://sia.storage`) |
| `recoveryPhrase` | Recovery phrase to register with, read on the first start only |
| `preauthorizedKey` | Pre-authorized key in hex, read on the first start only |
| `config` | Additional `s3d.yml` settings, for example `sia.dataShards` or `s3.hostBases` |
| `image.tag` | Image tag (default: the chart `appVersion`) |
| `persistence.size` | Size of the data volume (default `10Gi`) |
| `persistence.storageClass` | Storage class of the data volume (default: the cluster default) |
| `resources` | Container resources |
| `ingress.enabled` | Expose the S3 API through an Ingress (default `false`) |
| `ingress.hosts` | Ingress host names, add the wildcard for virtual-hosted-style buckets together with `config.s3.hostBases` |
| `ingress.className`, `ingress.annotations`, `ingress.tls` | Passed through to the Ingress |
| `nodeSelector`, `tolerations`, `affinity` | Passed through to the pod |
| `nameOverride`, `fullnameOverride` | Replace the chart name in resource names |

The admin API listens on port 8001 of the service, behind the password and
without TLS, so leave it inside the cluster. The S3 API is on port 8000 and is
the only port the Ingress exposes.

## Users and access keys

```sh
kubectl exec s3d-0 -- s3d users create <username>
kubectl exec s3d-0 -- s3d keys create <username>
```

The secret key is only shown once.
