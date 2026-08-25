# Starkstream DNA release contract

This repo publishes an immutable `starkstream-dna-starknet` runtime image for the
Starkstream infrastructure. On `main`, `.github/workflows/release.yml` owns the
image build and publishes the immutable
`<region>-docker.pkg.dev/<project>/dna-starknet/dna-starknet` image. It does not
deploy to Kubernetes or receive GKE credentials, `kubectl` access, or Terraform
state access.

The infra repo alone owns production deployment. It promotes the published image
by digest, always using `dna-starknet@sha256:...`; rollback restores a previous
digest without rebuilding.

## Networks

Testnet and mainnet are pinned separately but released in one run. The workflow
builds the image once and asks the infra repo to open both promotions as a stack
of pull requests: testnet branches off `main`, and mainnet branches off testnet.
A single digest feeds both, so the two networks always end up pinned to the very
same image.

That stack is what orders the two promotions, which is why no separate check
guards them: a mainnet merge can only reach `main` through the testnet one. Merge
the testnet pull request, verify the network, then merge the mainnet one — GitHub
retargets it onto `main` when testnet merges, so its diff narrows to the mainnet
manifest. The infra repo also registers the pair as a GitHub stack, so the two
show up as one reviewable unit rather than two unrelated pull requests. This binds
the release path, not the manifest: a mainnet rollback is still an ordinary
reviewed pull request against the infra repo.

A re-dispatch of an already released commit does not rebuild; the published image
is reused. Both paths name the released digest by reading the `git-<sha>` tag back
from the registry, and the build path additionally fails if that tag does not
resolve to what it just pushed. A tag that moved is therefore a failed release,
not a promotion of a digest this workflow never produced.

Keeping the tag from moving in the first place is the registry's job: the
`dna-starknet` Artifact Registry repository must be configured with immutable
tags, since the release identity depends on `git-<sha>` still naming the image
built for that commit. The readback above is what makes a lapse there loud
instead of silent.

Rolling back is a change to the release manifest in the infra repo, not a run of
this workflow.

## Release configuration

The workflow is started manually from the Actions page and only accepts the
`main` branch. `CI Pipeline` runs on every push to `main`, so each `main` commit
carries its own run; the workflow reads that run and refuses to build a commit
whose CI failed, is still running, or never ran at all — which is what a commit
pushed straight to `main` looks like. It looks for the `CI Pipeline` run
specifically, so an unrelated green check on the commit does not stand in for CI,
and it ignores its own runs, so a failed release attempt never blocks a later
one.

Configure these repository values before enabling the release workflow:

- variables `GCP_PROJECT_ID`, `GCP_REGION`, `WIF_PROVIDER`,
  `WIF_SERVICE_ACCOUNT`, and `INFRA_APP_ID`;
- secret `INFRA_APP_PRIVATE_KEY`.

The GitHub App installation must be limited to the infra repo with only
repository Contents write permission, which GitHub requires to send repository
dispatch events. The Google service account and Workload Identity provider are
created by the infra repo; that identity can only publish to the dedicated
`dna-starknet` Artifact Registry repository.
