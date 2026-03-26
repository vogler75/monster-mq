# Contributing to MonsterMQ

Thank you for your interest in contributing to MonsterMQ. This document explains how to contribute and what is required before your contribution can be accepted.

---

## Copyright Assignment Requirement

**All contributions to MonsterMQ require a signed Copyright Assignment Agreement.**

By submitting a contribution you irrevocably assign all intellectual property rights in that contribution — including copyright — to **Andreas Vogler**. This is a legal requirement; pull requests will not be merged until a signed agreement has been received.

### Steps

1. Download the agreement: [`doc/COPYRIGHT_ASSIGNMENT.pdf`](doc/COPYRIGHT_ASSIGNMENT.pdf)
2. Fill in your **full legal name**, **address**, and the **date**.
3. Sign the document (handwritten signature on a printed copy, or a qualified electronic signature).
4. Scan or photograph the signed document and e-mail it to the project maintainer, **or** attach it as a comment on your pull request.

> **Why?** A full copyright assignment allows Andreas Vogler to relicense, commercialize, and legally protect MonsterMQ without encumbrance. Your contribution will still be reflected in the project history and you will be credited.

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
   - **Copyright Assignment** — confirm you have signed and submitted the agreement.

Pull requests that do not include a signed Copyright Assignment Agreement will not be merged.

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
