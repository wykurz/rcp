# TEST SSH KEYS - DO NOT USE IN PRODUCTION

⚠️ **WARNING: These SSH keys are for TESTING ONLY** ⚠️

## What are these keys for?

These SSH keys are used **exclusively** for the Docker-based multi-host integration tests in this directory. They enable passwordless SSH authentication between test containers.

## Security Notice

- ✅ **Safe to commit to version control** - These keys are publicly visible by design
- ❌ **NEVER use on real servers** - They have no security value
- ❌ **NEVER use for production** - They are compromised by being in git
- ❌ **NEVER copy to ~/.ssh/** on your actual machine - Only for Docker containers

## Why are they checked in?

1. **Reproducibility** - Everyone running tests gets the same environment
2. **Simplicity** - No need to generate keys before running tests
3. **Standard practice** - Common in test suites (OpenSSH, Docker examples, etc.)
4. **Isolation** - Only used in ephemeral, local Docker containers

## What if I need different keys?

If you want to regenerate these keys:

```bash
cd tests/docker/ssh_keys
rm id_ed25519 id_ed25519.pub
ssh-keygen -t ed25519 -f id_ed25519 -N "" -C "rcp-test-key"
chmod 600 id_ed25519
chmod 644 id_ed25519.pub config
```

Then rebuild the Docker containers:

```bash
cd ..
./test-helpers.sh rebuild
```

## Files

- `id_ed25519` - Private key (TEST ONLY)
- `id_ed25519.pub` - Public key
- `config` - SSH client configuration for test containers
- `README.md` - This file

## Alternative: Generate keys at runtime

If you prefer not to check in keys, you could modify the setup to generate them on first run. However, this adds complexity and makes tests less reproducible. For test infrastructure, checking in test keys is the pragmatic choice.

---

**Remember: These keys are worthless for security. They're checked into a public repository and should never touch real infrastructure.**
