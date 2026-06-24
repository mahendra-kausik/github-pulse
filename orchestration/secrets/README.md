# orchestration/secrets

Place the pipeline service-account key here as `sa-key.json`. It is mounted read-only into the
Kestra container at `/secrets/sa-key.json` (see `../docker-compose.yml`).

Create it after `terraform apply`:

```bash
gcloud iam service-accounts keys create orchestration/secrets/sa-key.json \
  --iam-account "$(cd ../terraform && terraform output -raw service_account_email)"
```

**Never commit this key** — `*key*.json` is gitignored. This README is the only tracked file here.
