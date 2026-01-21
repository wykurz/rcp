# CHANGELOG Update Instructions

Update CHANGELOG.md for release ${VERSION}:

1. Review commits since ${LAST_TAG} using `git log ${LAST_TAG}..HEAD --oneline`
2. Move meaningful user-facing changes from [Unreleased] to a new section:
   ## [${VERSION}] - ${DATE}
3. Organize changes by category (keep only sections that have content):
   - **Added** - new features
   - **Changed** - changes in existing functionality
   - **Fixed** - bug fixes
   - **Removed** - removed features
4. Update the comparison links at the bottom:
   - Add: [${VERSION}]: https://github.com/wykurz/rcp/compare/${LAST_TAG}...v${VERSION}
   - Update [Unreleased] link to compare from v${VERSION}
5. Keep an empty [Unreleased] section at the top

Style guidelines:
- Be concise - one line per change
- Focus on user-facing changes, skip internal refactors unless significant
- Start entries with a verb (Add, Fix, Change, Remove, Improve)
- Reference issue/PR numbers where relevant
- Follow the existing CHANGELOG.md style and formatting
