# Security

## Reporting a vulnerability

E-mail **security@alpheus-dev.org**. Please do not report a vulnerability
in a public issue, a pull request or a discussion.

Tell us, as far as you know it:

- which part is affected: the controller, the agent, the `alpheus` CLI,
  `alpheus ui`, the SDKs, the Helm chart or the images;
- the version or commit;
- what an attacker can do, and what they need first, such as access to a
  namespace, the run's storage keys or the network;
- how to reproduce it.

If the details are sensitive, say so in a first message without them, and
we will agree on a way to send them.

## What happens then

- We confirm that the report arrived within **5 working days**.
- Within **30 days** we tell you whether we consider it a vulnerability,
  how serious we think it is, and when we expect a fix.
- We fix it in a new patch release of the supported version, and publish
  an advisory with the release notes and at
  https://alpheus-dev.org/security/. We agree the date with you, and
  credit you there if you want.

Please give us the time to release the fix before you publish the
details.

## Supported versions

Alpheus is before 1.0. Only the **latest minor version** (the newest
`vX.Y`) gets security fixes, as patch releases (`vX.Y.Z`); to get a fix
in an older version, upgrade to the latest.

No version is released yet, so no version is supported yet. When there
is, the page of each release under https://alpheus-dev.org/releases/ will
say how long it gets fixes.

## Verifying a release

Releases are signed with the project's own key and come with SBOMs and a
signed list of checksums. See https://alpheus-dev.org/docs/verify/ for how
to check a release, an image and the chart.

## Not in scope

The test cluster in `vagrant/` is for development on one machine. It is
reachable only from that machine, and some of its services have no login
on purpose (see `vagrant/README.md`). Do not run it as a shared or
production cluster.
