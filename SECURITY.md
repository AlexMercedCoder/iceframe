# Security policy

Please report suspected vulnerabilities privately through GitHub's security
advisory flow for this repository. Do not open a public issue containing
credentials, exploit details, catalog URLs, or customer data.

The maintained line is the latest release on `main`. Security fixes are made
there first; backports are considered when an older release is still widely
used. IceFrame is currently alpha and should be deployed with least-privilege
catalog credentials. The MCP surface is read-only by default and must never be
granted access to files or tables beyond those needed for its task.
