# Contributing to MonsterMQ

Thank you for your interest in contributing to MonsterMQ. This document explains how to contribute and what is required before your contribution can be accepted.

---

## Contributor License Agreement (CLA)

**All contributions to MonsterMQ require a signed Contributor License Agreement.**

The CLA grants the project maintainer the necessary rights to use, distribute, and sublicense your contributions — including in commercial and enterprise editions of MonsterMQ. **You retain full copyright ownership of your work.** This is the same approach used by projects like Apache, Elasticsearch, and Grafana.

### Steps

1. Download the agreement: [`doc/CONTRIBUTOR_LICENSE_AGREEMENT.pdf`](doc/CONTRIBUTOR_LICENSE_AGREEMENT.pdf)
2. Fill in your **full legal name**, **email**, **GitHub username**, and the **date**.
3. Sign the document (handwritten signature on a printed copy, or an electronic signature).
4. Scan or photograph the signed document and e-mail it to the project maintainer, **or** attach it as a comment on your pull request.

> **Why a CLA?** The CLA ensures the maintainer has a clear legal right to distribute all contributions as part of the Project — under the current open-source license and under any future license, including commercial editions. You keep ownership and can use your code elsewhere however you like.

---

## Reporting Issues

- Search existing issues before opening a new one.
- Use the provided issue templates where available.
- Include the MonsterMQ version (Docker image tag or git SHA), your configuration excerpt (redact credentials), and full log output around the error.
- For security vulnerabilities, do **not** open a public issue — contact the maintainer directly.

---

## Submitting a Pull Request

1. **Fork** the repository and create your branch from `main`.
2. Make your changes, adding or updating tests as appropriate.
3. Open a pull request against `main`. In the PR description include:
   - **Summary** — what problem does this solve?
   - **Changes** — list of files/components affected.
   - **Testing** — how did you verify the change?
   - **CLA** — confirm you have signed and submitted the Contributor License Agreement.

Pull requests that do not include a signed CLA will not be merged.

---

## Commit Conventions

Follow the [Conventional Commits](https://www.conventionalcommits.org/) style for commit messages:

```
<type>(<scope>): <short summary>
```

Keep the subject line under 72 characters, use the imperative mood, and reference issue numbers where applicable (`Closes #123`).

---

## Questions?

Open a [GitHub Discussion](../../discussions) or file an issue with the label `question`.
