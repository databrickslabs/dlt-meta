#!/usr/bin/env bash
#
# Build the Docusaurus site from the latest `main` and deploy it to the
# orphan `gh-pages` branch. GitHub Pages serves the branch root at:
# https://databrickslabs.github.io/sdp-meta/
#
# The source documentation remains on `main`; `gh-pages` contains only the
# rendered static site. A sibling worktree at ../sdp-meta-gh-pages keeps the
# deployment branch isolated from the main checkout.
#
# Run this from anywhere inside the repository:
#   .github/scripts/deploy-docs.sh
#
# Prerequisites:
#   - Node.js 20 or newer and npm 11.6.2
#   - GitHub Pages configured for:
#       Source = Deploy from a branch
#       Branch = gh-pages
#       Folder = / (root)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
WORKTREE_DIR="${REPO_ROOT}/../sdp-meta-gh-pages"
BUILD_DIR="${REPO_ROOT}/docs/build"

cd "${REPO_ROOT}"

# Preserve the caller's branch and uncommitted work.
ORIGINAL_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
STASHED=0

cleanup() {
  set +e
  cd "${REPO_ROOT}"
  current_branch="$(git symbolic-ref --short -q HEAD || true)"
  if [[ -n "${current_branch}" && "${current_branch}" != "${ORIGINAL_BRANCH}" ]]; then
    echo "==> Switching back to ${ORIGINAL_BRANCH}"
    git checkout "${ORIGINAL_BRANCH}"
  fi
  if [[ "${STASHED}" -eq 1 ]]; then
    echo "==> Restoring stashed changes"
    git stash pop
  fi
}
trap cleanup EXIT

if ! git diff --quiet ||
  ! git diff --cached --quiet ||
  [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
  echo "==> Stashing uncommitted changes"
  git stash push \
    --include-untracked \
    --message "deploy-docs.sh auto-stash $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  STASHED=1
fi

echo "==> Switching to main and pulling latest"
git checkout main
git pull --ff-only origin main

echo "==> Installing documentation dependencies"
(
  cd "${REPO_ROOT}/docs"
  node --version
  npm --version
  npm ci --no-audit --no-fund
)

echo "==> Building Docusaurus site"
(
  cd "${REPO_ROOT}/docs"
  npm run build
)

if [[ ! -f "${BUILD_DIR}/index.html" ]]; then
  echo "Error: build did not produce ${BUILD_DIR}/index.html" >&2
  exit 1
fi

git fetch origin gh-pages 2>/dev/null || true

if [[ ! -d "${WORKTREE_DIR}" ]]; then
  if git rev-parse --verify --quiet origin/gh-pages > /dev/null; then
    echo "==> Setting up gh-pages worktree from origin/gh-pages"
    git worktree add "${WORKTREE_DIR}" gh-pages
  else
    echo "==> Bootstrapping orphan gh-pages branch"
    git branch -D gh-pages 2>/dev/null || true

    if git worktree add --orphan -b gh-pages "${WORKTREE_DIR}" 2>/dev/null; then
      :
    else
      git worktree add -B gh-pages-bootstrap "${WORKTREE_DIR}" main
      (
        cd "${WORKTREE_DIR}"
        git checkout --orphan gh-pages
        git rm -rf . > /dev/null
      )
      git branch -D gh-pages-bootstrap 2>/dev/null || true
    fi

    (
      cd "${WORKTREE_DIR}"
      find . -mindepth 1 -maxdepth 1 ! -name ".git" -exec rm -rf {} +
      git commit --allow-empty -m "Initialize gh-pages"
    )
  fi
elif git rev-parse --verify --quiet origin/gh-pages > /dev/null; then
  echo "==> Resetting gh-pages worktree to origin/gh-pages"
  git -C "${WORKTREE_DIR}" reset --hard origin/gh-pages
fi

echo "==> Replacing gh-pages contents"
find "${WORKTREE_DIR}" \
  -mindepth 1 \
  -maxdepth 1 \
  ! -name ".git" \
  -exec rm -rf {} +
cp -R "${BUILD_DIR}/." "${WORKTREE_DIR}/"
touch "${WORKTREE_DIR}/.nojekyll"

echo "==> Committing and pushing gh-pages"
cd "${WORKTREE_DIR}"
git add -A

if git diff --cached --quiet; then
  echo "No changes to deploy. gh-pages is already up to date."
else
  SOURCE_SHA="$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
  git commit \
    -m "Deploy site from main@${SOURCE_SHA} on $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  git push -u origin gh-pages
  echo "==> Site will be live at https://databrickslabs.github.io/sdp-meta/ shortly."
fi

# cleanup() restores the original branch and any stashed work.
