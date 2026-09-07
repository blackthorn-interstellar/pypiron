---
description: What the pypiron documentation site and package server send over the network, including the advertising pixel and optional upstream services.
---

# Privacy

*Last updated: September 2026*

## Documentation site

Blackthorn Interstellar publishes this static site. It has no accounts or
forms. It loads OpenAI's `oaiq`
advertising pixel from `bzrcdn.openai.com`. The pixel records page views and
standard browser request data such as your IP address, user agent, and referring
page. On pages with install commands, it also records copy and selection events.
We use the data to measure advertising and documentation traffic.

OpenAI processes this data under its
[privacy policy](https://openai.com/policies/privacy-policy/). We do not sell it
or combine it with other information about you.

You can block the pixel with a tracker blocker. Browser third-party-cookie and
privacy settings also limit what it can record.

**California residents:** you may opt out of sharing your personal information
for cross-context behavioral advertising by emailing
[privacy@pypiron.com](mailto:privacy@pypiron.com). We will honor the request.

## Package server

**pypiron has no product telemetry, account service, or update check.** It does
not send package names, download counts, credentials, or configuration to
pypiron.com.

The server makes network requests required by the features you configure:

- By default, it polls the public OSV PyPI advisory feed for malware and
  vulnerability data. Set `--advisory-feed ""` to disable the feed and the
  organization audit.
- With `--proxy-upstream`, it requests public package listings and files from
  that upstream when clients ask for them.
- Object-storage deployments read and write the S3, GCS, Azure, or compatible
  endpoint you configure.
- On cloud VMs, pypiron may query the provider's instance metadata for
  credentials and its region.
- `pypiron sync` contacts its configured source and destination.

Access logs, download statistics, and package metadata stay in your server and
storage. Your cloud provider, upstream package index, forward proxy, or network
operator may retain its own request records under its own policy.

Questions: [privacy@pypiron.com](mailto:privacy@pypiron.com).
